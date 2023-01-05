"""
Implements the main command line interface of RedSQL
"""

import argparse
import logging
import logging.config
import os
import signal
import string
import time
from typing import Optional

import dotenv
import prometheus_client as prom
import redis
import sqlalchemy as sql
import yaml

import redsql.channel_executor as executor

logger = logging.getLogger(__name__)


def main(argv=None, prog=None):
    """
    Parses the commandline arguments, reads the configuration and starts the main program flow

    :param argv:
    :param prog:
    """

    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog=prog, description="Feeds data from Redis to some SQL database")
    parser.add_argument("--config_file", metavar="CONF", default="redsql.yaml",
                        help="The main YAML configuration describing the translation process")
    parser.add_argument("--env", metavar="ENV_FILE", default=None,
                        help="An environment file that specifies the variables to load")
    args = parser.parse_args(args=argv)

    # Load the environment variables and system configuration
    load_env_file(args.env)
    logger.debug("Parse main YAML configuration file '%s'", args.config_file)
    config = load_config(args.config_file)

    _setup_logging(config.get("logging", None))
    _startup_prometheus_client(config.get("prometheus client", {}))

    redis_pool = _load_redis_connection_pool(config)
    sql_engine = _load_db_engine(config)

    supervisor = executor.ChannelSupervisor(config["channels"], redis_pool, sql_engine)
    supervisor.start()

    logger.info(f"Startup of channels {supervisor.channel_names} complete, press Ctrl+C to exit the data crawler.")
    _heartbeat_until_termination_request(supervisor)

    logger.info(f"Begin to shutdown the data crawler.")
    supervisor.stop()
    logger.info("Bye!")


def _setup_logging(config: Optional[dict]):
    """
    Setups the logging module according to the given configuration.

    In case no config is given, a default one is used. The documentation fo the logging scheme can be found at
    https://docs.python.org/3.9/library/logging.config.html#configuration-dictionary-schema

    :param config: The configuration snippet to apply
    """

    logging_config = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default_fmt",
                "level": "DEBUG",
                "stream": "ext://sys.stdout"
            }
        },
        "formatters": {
            "default_fmt": {
                "format": "%(asctime)s %(name)s %(levelname)s: %(message)s"
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console"]
        }
    }

    if config is not None:
        logging_config.update(config)

    logging.config.dictConfig(logging_config)


def _startup_prometheus_client(prometheus_config: Optional[dict] = None):
    """Starts a local webserver that exposes the internal metrics, if requested"""
    if prometheus_config is not None:
        port = prometheus_config.get("port", 8000)
        prom.start_http_server(port=port)
        logger.info(f"Started the prometheus server at http://localhost:{port}")


def _heartbeat_until_termination_request(supervisor: executor.ChannelSupervisor):
    """suspends the main thread until a termination request was received"""

    def _handler(signal_number, _frame):
        logger.debug(f"Received signal {signal_number}. Initiate shutdown.")
        raise KeyboardInterrupt("The end is near!")

    # Install the signal handlers
    signal_codes = ("SIGTERM", "SIGINT", "SIGBREAK", "SIGHUP")
    for code in signal_codes:
        # Some signals are not defined on Unix/Windows :-(
        signal_nr = getattr(signal, code, None)
        if signal_nr is not None:
            signal.signal(signal_nr, _handler)

    # Sleep until a KeyboardInterrupt it caught
    # Using an event rather than an exception would be nicer, but exit_event.wait() blocks the signal handler.
    try:
        while True:
            time.sleep(10)
            supervisor.heartbeat()
    except KeyboardInterrupt:
        pass


def _startup_executors(config: dict, redis_pool: redis.ConnectionPool, sql_engine: sql.engine.Engine) -> dict:
    """Creates the executors and starts them"""

    channel_config: dict = config["channels"]
    channel_executors = {
        ex_name: executor.ThreadChannelExecutor(cnf, redis_pool, sql_engine, ex_name)
        for ex_name, cnf in channel_config.items()
    }

    for ex in channel_executors.values():
        ex.start()
    return channel_executors


def _stop_executors(channel_executors: dict):
    """Stops the channel executors and waits until they are finished"""

    for ex in channel_executors.values():
        ex.stop()

    for ex in channel_executors.values():
        ex.join()


def _load_db_engine(config) -> sql.engine.Engine:
    """Parses the configuration to load the DB engine"""

    engine_url = config["database connection"]
    engine = sql.create_engine(engine_url)
    engine.connect()
    logger.debug(f"Connected to the database {engine.name}.")

    return engine


def _load_redis_connection_pool(config: dict) -> redis.ConnectionPool:
    """Parses the configuration and instantiates the Redis connection pool"""

    redis_config: dict = config["redis"]
    host = redis_config["host"]
    port = redis_config["port"]
    db = redis_config["db"]
    logger.debug(f"Configure redis connection to {host}:{port} using db {db}")

    if os.getenv('REDSQL_REDIS_PASSWORD') is not None:
        pool = redis.ConnectionPool(host=host, port=port, db=db, password=os.getenv('REDSQL_REDIS_PASSWORD'),
                                    decode_responses=True)
    else:
        pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)

    client = redis.Redis(connection_pool=pool)
    client.ping()  # Will raise an exception in case a connection error occurs
    logger.debug(f"Redis connection to {host}:{port} using db {db} is alive.")

    return pool


def load_env_file(env_file: Optional[str]) -> None:
    """
    Populates the environment vairables by elements stored in the .env file

    :param env_file: The path of the .env file or None, in case nothing should be changed
    """

    if env_file is not None:
        logger.debug("Load environment file '%s'", env_file)
        dotenv.load_dotenv(env_file)


def load_config(config_file: str) -> dict:
    """
    Parses the YAML configuration, preprocesses it and returns the resulting structure of dictionaries
    :param config_file: The path of the main configuration file
    :return: The obtained configuration as a structure of nested dicts as generated by the YAML loader
    """

    config_file = os.path.abspath(config_file)
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"The main configuration file '{config_file}' is not found")

    yaml.add_constructor("!env-template", _load_substitute_env)

    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.Loader)

    if not config.get("version", 1) == 1:
        raise SyntaxError("Invalid configuration version. Only version 1 is supported.")

    return config


def _load_substitute_env(loader, node):
    """Loads the YAML node by substituting environment variables using Python template syntax"""

    template_str = loader.construct_scalar(node)
    template = string.Template(template_str)
    return template.substitute(**os.environ)
