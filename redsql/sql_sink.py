"""
Implements the SQL table sink that pushes the data to the database
"""
import io
import logging
import time
from typing import Dict, Iterable, Any, List, Optional

import prometheus_client as prom
import pydantic
import sqlalchemy as sql
import sqlalchemy.exc

from redsql import exc as exc
import json


class TableConfig(pydantic.BaseModel):
    """Defines the configuration snippet for a single SQL table"""

    table: str = pydantic.Field(description="The name of the table within the database")
    columns: Dict[str, str] = pydantic.Field(description="The mapping of table columns to message keys", default={})

    update_duplicate_values: bool = pydantic.Field(
        description="Override duplicates in the table", default=False,
        validation_alias=pydantic.AliasChoices("update_duplicate_values", "update duplicate values")
    )


class _PrecompiledTableSink:
    """
    Represents a single precompiled table and the logic to insert the data into the table
    """

    def __init__(self, table_config: TableConfig, sql_meta: sql.MetaData, channel_name: str, table_id: str,
                 logger: logging.Logger):
        """
        Instantiates the precompiled table sink and prepares it for inserting data

        :param table_config: The configuration of the single table entry
        :param sql_meta: The populated meta-data of the database to fetch the table information from
        :param channel_name: The name of the associated channel for debugging and logging reasons
        :param table_id: The internal identifier for logging purpose. May not be identical to any table name
        :param logger: The logger to write out debug information
        """
        self._logger = logger
        self._destination_table = sql_meta.tables[table_config.table]
        self._column_mapping = self._get_column_mapping(table_config.columns, self._destination_table, channel_name)
        self._type_mapping = self._get_type_mapping(self._destination_table)
        self._logger.debug(f"Determine the column mapping of {table_id} (DB table {self._destination_table.name}): "
                           f"{self._column_mapping}")

        self._update_duplicates = table_config.update_duplicate_values
        self._insert_statement = self._compile_insert_statement(
            self._destination_table, self._logger,
            update_duplicates=self._update_duplicates
        )

        # Detect if destination is a view - COPY doesn't work with views
        self._is_view = self._destination_table.info.get('is_view', False) or len(list(self._destination_table.primary_key)) == 0

    @staticmethod
    def _get_column_mapping(column_config: dict, destination_table: sql.Table, channel_name: str) -> Dict[str, str]:
        """
        Returns the autocompleted mapping from table columns to message keys

        :param column_config: The partial column definition from the configuration mapping column names to message
            keys as well.
        :param destination_table: The SQL table definition
        :param channel_name: The channel name for debugging purpose
        """

        column_names = [col.name for col in destination_table.columns]

        invalid_config_columns = set(column_config.keys()).difference(column_names)
        if len(invalid_config_columns) > 0:
            raise KeyError(f"The configuration of channel {channel_name}, table {destination_table.name} contains "
                           f"invalid column names: {invalid_config_columns}. Available columns: {column_names}")

        mapping = {col_name: column_config.get(col_name, col_name) for col_name in column_names}
        return mapping

    @staticmethod
    def _get_type_mapping(destination_table: sql.Table) -> Dict[str, type]:
        """Returns a mapping form the column name to the actual python type (for caching)"""

        def default_cast(x):
            return x

        supported_casts = {
            tp.__name__: tp
            for tp in [float, int, bool, str]
        }

        col_casts = {}
        for col in destination_table.columns.keys():
            col_casts[col] = supported_casts.get(destination_table.columns[col].type.python_type.__name__,
                                                 default_cast)

        return col_casts

    @staticmethod
    def _compile_insert_statement(destination_table: sql.Table, logger: logging.Logger,
                                  update_duplicates: bool = False):
        """Compiles the SqlAlchemy insert statement according to the given configuration and returns it"""

        if update_duplicates:
            logger.debug(f"Update duplicate values in {destination_table.name}. This feature requires PostgreSQL.")
            import sqlalchemy.dialects.postgresql as pg_dialect  # Requires PostgreSQL

            ins_stmt = pg_dialect.insert(destination_table)
            primary_key_columns = _PrecompiledTableSink._infer_primary_key_columns(destination_table, logger)

            update_mapping = {
                # PG creates an intermediate excluded table for all invalid statements that needs to be referenced
                col: getattr(ins_stmt.excluded, col.name)
                for col in destination_table.columns if col.name not in primary_key_columns
            }
            ins_stmt = ins_stmt.on_conflict_do_update(index_elements=primary_key_columns, set_=update_mapping)
        else:
            ins_stmt = sql.insert(destination_table)

        logger.debug(f"Compiled insert statement: {ins_stmt}")
        return ins_stmt

    @staticmethod
    def _infer_primary_key_columns(destination_table: sql.Table, logger: logging.Logger) -> List[str]:
        """
        Tries to infer the primary key columns of the table and returns the result.

        For tables this can be done automatically. However, for views, only an empty primary key will be returned.
        Since we miss a sane way of querying the affected primary key columns automatically, and we don't want to break
        tons of existing code, some default rules will apply. In case have other needs and need them configured,
        consider opening a ticket.

        :param destination_table: The table to fetch the primary key columns from
        :return: The inferred list of primary key column names
        """

        known_views = {
            "measurements": ["dp_id", "obs_time"],
            "forecasts": ["dp_id", "obs_time", "fc_time"]
        }

        primary_keys = [c for c in destination_table.constraints if isinstance(c, sql.PrimaryKeyConstraint)]
        if len(primary_keys) != 1:
            raise ValueError(f"It is requested to update duplicate values but {destination_table.name} "
                             f"does not have a unique primary key: {primary_keys}")

        columns = list(primary_keys[0].columns)

        if len(columns) <= 0 and (
                destination_table.name not in known_views or
                set(destination_table.columns).issuperset(known_views[destination_table.name])
        ):
            raise KeyError(f"The destination table {destination_table.name} appears to have no primary key columns for "
                           "deduplication. This is likely on views that hide destination table. There is also no rule "
                           "set that enables default inference. Please consider opening a ticket if you really need to "
                           "update duplicates on that table/view.")
        elif len(columns) <= 0:
            columns = known_views[destination_table.name]
            logger.warning(f"The destination table {destination_table.name} appears to be a view. It is assumed that "
                           f"the primary key columns are {columns}. However, consider directly writing to the "
                           "destination table instead.")

        return columns

    def _deduplicate_batch(self, output_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicates a batch of messages by primary key, keeping only the last occurrence.

        This is necessary when using ON CONFLICT DO UPDATE because PostgreSQL doesn't allow
        updating the same row multiple times in a single statement.

        :param output_data_list: List of processed message dictionaries
        :return: Deduplicated list with only the last occurrence of each primary key
        """
        if not output_data_list:
            return output_data_list

        # Get the primary key columns for this table
        primary_key_columns = self._infer_primary_key_columns(self._destination_table, self._logger)

        # Build a dictionary keyed by primary key tuple, keeping last occurrence
        unique_rows = {}
        for row_data in output_data_list:
            # Create a tuple of primary key values for this row
            pk_values = tuple(row_data.get(col) for col in primary_key_columns)
            # Store the row, overwriting any previous row with the same key
            unique_rows[pk_values] = row_data

        # Return the deduplicated list in original order (using dict preservation of insertion order)
        return list(unique_rows.values())

    def insert(self, message: Dict[str, Any], sql_connection: sql.Connection):
        """
        Inserts the given message into the database

        :param message: The message to be inserted
        :param sql_connection: The open sql connection to execute the corresponding insert statements
        """

        output_data = self._remap_message(message)  # Apply the new column mapping
        output_data = self._cast_message(output_data)  # Convert to the appropriate python types

        try:
            sql_connection.execute(self._insert_statement, output_data)

        except sqlalchemy.exc.DataError as e:
            new_err = exc.MessageFormatError(f"Unable to insert samples into {self._destination_table.name} using "
                                             f"'{e.statement}' and params {e.params}: {e.detail}, {e.orig}.",
                                             triggering_message=e.params)
            raise new_err from e
        except sqlalchemy.exc.IntegrityError as e:
            new_err = exc.MessageFormatError(f"Integrity error when inserting samples into "
                                             f"{self._destination_table.name} using '{e.statement}' and params "
                                             f"{e.params}: {e.detail}, {e.orig}",
                                             triggering_message=e.params)
            raise new_err from e

    def insert_batch(self, messages: List[Dict[str, Any]], sql_connection: sql.Connection):
        """
        Inserts multiple messages into the database in a single batch operation

        :param messages: List of messages to be inserted
        :param sql_connection: The open sql connection to execute the corresponding insert statements
        """

        if not messages:
            return

        # Process all messages: remap and cast
        output_data_list = []
        for message in messages:
            output_data = self._remap_message(message)
            output_data = self._cast_message(output_data)
            output_data_list.append(output_data)

        # Only use COPY if we don't need update_duplicates because upsert does not work with copy
        # and we don't want to use COPY for views because it's not supported by PostgreSQL
        if not self._update_duplicates and not self._is_view:
            try:
                self._insert_with_copy(output_data_list, sql_connection)
                return
            except Exception as copy_err:
                self._logger.warning(f"COPY failed for {self._destination_table.name}: {copy_err}")

        # When using update_duplicates, deduplicate the batch to avoid cardinality violations
        # PostgreSQLs ON CONFLICT DO UPDATE doesn't allow updating the same row twice in one statement,
        # but existing ones are still updated
        if self._update_duplicates:
            output_data_list = self._deduplicate_batch(output_data_list)

        try:
            # Use insert for upserts
            sql_connection.execute(self._insert_statement, output_data_list)
        except sqlalchemy.exc.DatabaseError as e:
            raise exc.MessageFormatError(f"Unable to insert batch into {self._destination_table.name}: {e}") from e

    def _insert_with_copy(self, output_data_list: List[Dict[str, Any]], sql_connection: sql.Connection):
        """
        Uses PostgreSQL COPY command for batch insert if we really have a lot of data
        This is not really slower than the normal insert to it is the preferred method if
        the data does not need to be upserted.

        :param output_data_list: List of processed message dictionaries
        :param sql_connection: The open sql connection
        """
        if not output_data_list:
            return

        # Get ordered column names from the first message
        columns = list(output_data_list[0].keys())
        column_names = ', '.join(f'"{col}"' for col in columns)

        # Create tab-delimited data in memory
        buffer = io.StringIO()
        for row_data in output_data_list:
            row_values = []
            for col in columns:
                value = row_data.get(col)
                if value is None:
                    row_values.append('\\N')  # PostgreSQL NULL representation
                elif isinstance(value, str):
                    # Escape special characters for COPY format
                    escaped = value.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                    row_values.append(escaped)
                elif isinstance(value, (dict, list)):
                    # Handle JSONB columns
                    json_str = json.dumps(value).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                    row_values.append(json_str)
                else:
                    row_values.append(str(value))
            buffer.write('\t'.join(row_values) + '\n')

        # Reset buffer position to beginning
        buffer.seek(0)

        # Get raw connection for COPY
        raw_connection = sql_connection.connection.driver_connection
        cursor = raw_connection.cursor()

        try:
            # Execute COPY command
            copy_sql = f'COPY {self._destination_table.name} ({column_names}) FROM STDIN'
            self._logger.debug(f"Executing COPY command: {copy_sql} with: \n{buffer.getvalue()}")
            cursor.copy_expert(copy_sql, buffer)
        finally:
            cursor.close()
            buffer.close()

    def _remap_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts the column values from the message and returns them"""

        sample_data = {
            col_name: message[message_name]
            for col_name, message_name in self._column_mapping.items() if message_name in message
        }
        return sample_data

    def _cast_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms the field type to the corresponding python type before inserting"""

        ret = {}
        for column_name, source_value in message.items():
            try:
                if source_value is None:
                    ret[column_name] = None
                else:
                    ret[column_name] = self._type_mapping[column_name](source_value)
            except ValueError as err:
                new_err = exc.MessageFormatError(
                    f"Unable to cast the value of column {column_name} from {self._destination_table.name} to "
                    f"{self._type_mapping[column_name].__name__}. Got an invalid value '{source_value}'.",
                    triggering_message=message
                )
                raise new_err from err
            except TypeError as err:
                new_err = exc.MessageFormatError(
                    f"Unable to cast the value of column {column_name} from {self._destination_table.name} to "
                    f"{self._type_mapping[column_name].__name__}. Got an invalid value '{source_value}'.",
                    triggering_message=message
                )
                raise new_err from err

        return ret


class SQLTableSink:
    """
    Helper class that allows to store messages in database tables

    The class is specifically optimized to exploit the SQLAlchemy insertion mechanisms instead of less performant but
    more generic SQL queries.
    """

    _prom_insert_cnt = prom.Counter("redsql_inserted_table_rows", labelnames=["channel_name"],
                                    documentation="Number of inserted or updated table rows")

    def __init__(self, config: dict, sql_engine: sql.engine.Engine, channel_name: str):
        """

        :param config: The SQL-specific configuration stanza
        :param sql_engine: The DB engine to draw the connections from.
        :param channel_name: The name of the corresponding channel for debugging purpose
        """

        self._channel_name = channel_name
        self._logger = logging.getLogger(f"{__name__}.{channel_name}")

        self._logger.debug(f"Try to access meta data from the SQL engine {sql_engine}")
        self._sql_engine = sql_engine
        self._sql_meta = sql.MetaData()
        # views need to be set to True, otherwise they will not be shown in the list of tables (#49)
        self._sql_meta.reflect(bind=sql_engine, views=True)
        self._logger.debug(f"SQL schema successfully retrieved.")

        table_config = self._get_table_config(config, channel_name)
        self._table_sinks = {
            table_id: _PrecompiledTableSink(table_config, self._sql_meta, channel_name, table_id, self._logger)
            for table_id, table_config in table_config.items()
        }

        self._table_key = config.get("table_key", None)
        if self._table_key is None and "_default" not in self._table_sinks:
            raise KeyError(f"No table_key given but also no _default table configured at {channel_name}")
        if self._table_key is None and len(self._table_sinks) != 1:
            raise KeyError(f"There are multiple tables configured but no table_key listed to select them at "
                           f" channel {channel_name}")

        self._prom_insert_cnt.labels(channel_name=channel_name)

        # Time-based batching: configurable via batch_interval (defaults to None = immediate insert)
        self._batch_buffer = []  # Accumulated messages
        self._batch_start_time = None  # When current batch started
        self._batch_interval = config.get("batch_interval", None)  # Flush interval in seconds (None = no batching)

    def _group_messages_by_sink(self, messages: List[Dict[str, Any]]) -> Dict[int, Dict]:
        """
        Groups messages by their destination table sink.

        :param messages: List of messages to group
        :return: Dictionary mapping sink_id to {sink, messages}
        """
        messages_by_sink = {}
        for message in messages:
            sink = self._get_destination_table(message)
            sink_id = id(sink)
            if sink_id not in messages_by_sink:
                messages_by_sink[sink_id] = {"sink": sink, "messages": []}
            messages_by_sink[sink_id]["messages"].append(message)
        return messages_by_sink

    def _execute_with_connection(self, operation):
        """
        Executes the given operation with a database connection from the pool.

        :param operation: A callable that takes a connection as its only argument
        """
        # TODO: It seems like this is fast enough, maybe later we need one dedicated connection per channel
        with self._sql_engine.connect() as conn:
            with conn.begin():
                operation(conn)

    @staticmethod
    def _get_table_config(config: dict, channel_name: str) -> Dict[str, TableConfig]:
        """Resolves the configuration stanzas using the backwards-compatibility rules"""

        if "tables" in config:
            # New tables-based configuration
            if any(k in config for k in ["table", "columns", "update duplicate values"]):
                raise KeyError(f"The data sink configuration for {channel_name} contains both, the tables dictionary "
                               f"and direct table properties (table, columns, update duplicate values): "
                               f"{list(config.keys())}")

            return {
                tab_key: TableConfig.model_validate(tab_config)
                for tab_key, tab_config in config["tables"].items()
            }
        else:
            return {"_default": TableConfig.model_validate(config)}


    def insert_messages(self, messages: Iterable[Dict[str, Any]]):
        """
        Inserts the given messages into the database

        Before inserting, the keys will be mapped according to the columns section in the configuration. In case some
        columns are not defined in the configuration section, it is assumed that each message contains a key with the
        respective column name.

        If batch_interval is configured, messages are accumulated and flushed after the interval.
        If batch_interval is None (default), messages are inserted immediately.

        :param messages: An iterable of messages. Each message must be composed of generic key-value pairs.
        """

        self._logger.debug(f"Start to compute table representation and insert samples for channel {self._channel_name}")
        output_data = list(messages)

        # If no batching configured, insert immediately
        if self._batch_interval is None:
            self._insert_immediate(output_data)
            return

        # Batching enabled: add messages to buffer
        self._batch_buffer.extend(output_data)

        # Initialize batch timer on first message
        if self._batch_start_time is None:
            self._batch_start_time = time.time()

        # Check if it's time to flush
        elapsed = time.time() - self._batch_start_time
        if elapsed >= self._batch_interval:
            self._flush_batch()

    def _insert_immediate(self, output_data: List[Dict[str, Any]]):
        """Immediately inserts messages - uses batch insert if no updates required, otherwise one-by-one"""
        self._logger.debug(f"Begin to insert {len(output_data)} row(s) immediately for {self._channel_name}")

        # Group messages by destination table
        messages_by_sink = self._group_messages_by_sink(output_data)

        def do_insert(conn):
            for sink_data in messages_by_sink.values():
                sink = sink_data["sink"]
                batch_messages = sink_data["messages"]
                sink.insert_batch(batch_messages, conn)

        self._execute_with_connection(do_insert)

        self._prom_insert_cnt.labels(channel_name=self._channel_name).inc(len(output_data))
        self._logger.debug(f"Successfully inserted {len(output_data)} row(s) for {self._channel_name}")

    def _flush_batch(self):
        """Flushes the accumulated batch to the database"""

        if not self._batch_buffer:
            return

        batch_size = len(self._batch_buffer)
        self._logger.debug(f"Flushing batch of {batch_size} messages for {self._channel_name}")

        # Group messages by destination table
        messages_by_sink = self._group_messages_by_sink(self._batch_buffer)

        def do_batch_insert(conn):
            for sink_data in messages_by_sink.values():
                sink = sink_data["sink"]
                batch_messages = sink_data["messages"]
                self._logger.debug(f"Batch inserting {len(batch_messages)} messages to table {sink._destination_table.name}")
                sink.insert_batch(batch_messages, conn)

        self._execute_with_connection(do_batch_insert)

        self._prom_insert_cnt.labels(channel_name=self._channel_name).inc(batch_size)
        self._logger.debug(f"Successfully inserted {batch_size} row(s) for {self._channel_name}")

        # Clear the buffer and reset timer
        self._batch_buffer = []
        self._batch_start_time = None

    def flush(self):
        """Public method to force flush the current batch"""
        self._flush_batch()

    def _get_destination_table(self, message) -> _PrecompiledTableSink:
        """Resolves the table sink based on the given message properties"""

        if self._table_key is None:
            return self._table_sinks["_default"]
        else:
            if self._table_key not in message:
                raise exc.MessageFormatError(f"The transformed message has no {self._table_key} field to "
                                             "select the output table", message)

            table_id = message[self._table_key]
            if table_id in self._table_sinks:
                return self._table_sinks[table_id]
            elif "_default" in self._table_sinks:
                return self._table_sinks["_default"]
            else:
                raise exc.MessageFormatError(f"Unknown table identifier '{table_id}' and no _default table is "
                                             "configured", message)

    def close(self):
        """Closes the database connection and frees allocated resources"""
        # Flush any remaining messages in the buffer
        self._flush_batch()
