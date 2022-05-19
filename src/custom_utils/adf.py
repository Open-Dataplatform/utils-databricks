"""Functions to help communicating with ADF"""

import json


def get_parameter(dbutils, parameter_name, default_value=''):
    """Creates a text widget and gets parameter value. If ran from ADF, the value is taken from there."""
    dbutils.widgets.text(parameter_name, default_value)
    return dbutils.widgets.get(parameter_name)


def get_config_parameter(dbutils, parameter_name, default_config: dict):
    """Gets the config and converts it to a dict. If ran in Databricks, default_config is used. If executed from ADF,
    it is checked whether the config from ADF is the same as default_config."""

    try:
        json_str_config = dbutils.widgets.get(parameter_name)
        config = json.loads(json_str_config)
    except:
        config = default_config

    return config


def get_source_config(dbutils, default_source_config):
    """Gets the source configuration and verifies it."""
    source_config = get_config_parameter(dbutils, 'SourceConfig', default_source_config)
    _verify_config(source_config)

    return source_config


def get_destination_config(dbutils, default_destination_config):
    """Gets the destination configuration and verifies it."""
    destination_config = get_config_parameter(dbutils, 'DestinationConfig', default_destination_config)

    if len(destination_config) > 1:
        raise Exception('You are only allowed to have one destination dataset.')

    _verify_config(destination_config)

    return destination_config


def _verify_config(config):
    """Runs through the dataset configs in a source/destination config and checks the schema.

    :param config:  An example could be {"<dataset_identifier>": {"type":"adls", "dataset":"<dataset_name>", "container":"landing", "account":"dplandingstorage"}}
    """

    for dataset_config in config.values():
        _verify_dataset_config(dataset_config)


def _verify_dataset_config(dataset_config):
    """Check the schema of a dataset config

    :param dataset_config:  An example could be {"type":"adls", "dataset":"<dataset_name>", "container":"landing", "account":"dplandingstorage"}
    """
    assert 'type' in dataset_config

    if dataset_config['type'] == 'adls':
        assert 'dataset' in dataset_config
        assert 'container' in dataset_config
        assert 'account' in dataset_config
    else:
        raise Exception(f'{dataset_config["type"]} is not a valid dataset type.')
