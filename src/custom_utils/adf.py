"""Functions to help communicating with ADF"""

def get_parameter(parameter_name, default_value=''):
    """Creates a text widget and gets parameter value. If ran from ADF, the value is taken from there."""
    dbutils.widgets.text(parameter_name, default_value)
    return dbutils.widgets.get(parameter_name)


def get_parameter_json(parameter_name):
    """Gets parameter and converts it to a dict."""
    json_str_config = get_parameter(parameter_name, '{"": ""}')
    return json.loads(json_str_config)
