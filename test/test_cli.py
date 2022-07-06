"""
Tests the high-level CLI and its configuration utilities
"""

import os
from typing import Dict

import pytest

import redsql.cli as cli


@pytest.fixture()
def minimal_config_file() -> str:
    """Returns the path to a minimal configuration"""

    file_path = os.path.join(__file__, "../../data/test/minimal-config.yml")
    file_path = os.path.abspath(file_path)
    return file_path


@pytest.fixture()
def mockup_env_file() -> str:
    """Returns a simple mockup .env file"""

    file_path = os.path.join(__file__, "../../data/test/mockup.env")
    file_path = os.path.abspath(file_path)
    return file_path


@pytest.fixture()
def minimal_env_test_set() -> Dict[str, str]:
    """Temporarily updates the environment variables and returns the managed set"""

    env_set = {
        "DB_CRED": "nsa:backdoor",
        "DB_HOST": "database.ait.ac.at"
    }

    backup_set = {key: os.environ.get(key, None) for key in env_set.keys()}

    os.environ.update(env_set)
    yield env_set

    # Revert the changes to the environment variables to keep debugging sane
    for var_name, var_value in backup_set.items():
        if var_value is None:
            del os.environ[var_name]
        else:
            os.environ[var_name] = var_value


def test_load_config_minimal(minimal_config_file, minimal_env_test_set):
    """Loads and checks the minimal test config"""

    config = cli.load_config(minimal_config_file)
    assert config is not None
    assert "version" in config
    assert config["version"] == 1

    assert "channels" in config
    assert config["channels"] is not None


def test_load_config_env_template(minimal_config_file, minimal_env_test_set):
    """Tests the environment variable_substitution"""

    config = cli.load_config(minimal_config_file)

    assert "database connection" in config
    assert "postgresql://nsa:backdoor@database.ait.ac.at" == config["database connection"]


def test_load_env_mockup(mockup_env_file, minimal_env_test_set):
    """Tests loading the environment file"""

    cli.load_env_file(mockup_env_file) # No override of existing variables
    assert os.environ["DB_CRED"] == "nsa:backdoor"
    assert os.environ["DB_HOST"] == "database.ait.ac.at"

    del os.environ["DB_CRED"]
    del os.environ["DB_HOST"]

    cli.load_env_file(None)
    assert "DB_CRED" not in os.environ
    assert "DB_HOST" not in os.environ

    cli.load_env_file(mockup_env_file)
    assert os.environ["DB_CRED"] == "user:not-a-real-pwd"
    assert os.environ["DB_HOST"] == "database-host"
