"""
Implements the main command line interface of RedSQL
"""

import argparse
import logging

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
    args = parser.parse_args(args=argv)

    logger.debug("Parse main YAML configuration file '%s'", args.config_file)

    pass  # TODO: Implement the main program flow
