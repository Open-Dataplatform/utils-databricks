"""Functions to help communicating with ADF"""

import json


DEFAULT_CONFIG_PARAMETER = '{"EXAMPLE":{"type":"adsl","dataset":"EXAMPLE","container":"EXAMPLE"}}'


def get_parameter(dbutils, parameter_name, default_value=''):
    """Creates a text widget and gets parameter value. If ran from ADF, the value is taken from there."""
    dbutils.widgets.text(parameter_name, default_value)
    return dbutils.widgets.get(parameter_name)


def get_config_parameter(dbutils, parameter_name):
    """Gets parameter and converts it to a dict."""
    json_str_config = get_parameter(dbutils, parameter_name, DEFAULT_CONFIG_PARAMETER)
    return json.loads(json_str_config)


def initialize_config_widgets(dbutils):
    """Initializes text widgets for source and destination configs."""
    dbutils.widgets.text('SourceConfig', DEFAULT_CONFIG_PARAMETER)
    dbutils.widgets.text('DestinationConfig', DEFAULT_CONFIG_PARAMETER)


def get_source_config(dbutils):
    """Gets the source configuration and verifies it."""
    source_config = get_config_parameter(dbutils, 'SourceConfig')
    _verify_config(source_config)

    return source_config


def get_destination_config(dbutils):
    """Gets the destination configuration and verifies it."""
    destination_config = get_config_parameter(dbutils, 'DestinationConfig')

    if len(destination_config) > 1:
        raise Exception('You are only allowed to have one destination dataset.')

    _verify_config(destination_config)

    return destination_config


def _verify_config(config):
    """Runs through the dataset configs in a source/destination config and checks the schema.

    :param config:  An example could be {"<dataset_identifier>": {"type":"adsl","dataset":"<dataset_name>","container":"landing"}}
    """
    if 'EXAMPLE' in config:
        raise Exception("It looks like you are using an example configuration. If developing in Databricks, manually fill out the source and destination configuration widgets and run the notebook again.")

    for dataset_config in config.values():
        _verify_dataset_config(dataset_config)


def _verify_dataset_config(dataset_config):
    """Check the schema of a dataset config

    :param dataset_config:  An example could be {"type":"adsl","dataset":"<dataset_name>","container":"landing"}
    """
    assert 'type' in dataset_config

    if dataset_config['type'] == 'adsl':
        assert 'dataset' in dataset_config
        assert 'container' in dataset_config
    else:
        raise Exception(f'{dataset_config["type"]} is not a valid dataset type.')
