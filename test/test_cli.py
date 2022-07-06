"""
Tests the high-level CLI and its configuration utilities
"""

import os

import pytest

import redsql.cli as cli


@pytest.fixture()
def minimal_config_file():
    """Returns the path to a minimal configuration"""

    file_path = os.path.join(__file__, "../../data/test/minimal-config.yml")
    return file_path


def test_load_config_minimal(minimal_config_file):
    """Loads and checks the minimal test config"""

    config = cli.load_config(minimal_config_file)
    assert config is not None
    assert "version" in config
    assert config["version"] == 1

    assert "channels" in config
    assert config["channels"] is not None
