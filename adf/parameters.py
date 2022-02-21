"""
Contains functionality to get parameters from ADF
"""

from py4j.protocol import Py4JJavaError

def get(parameter_name, default_value, dbutils):
    """Returns parameter from ADF"""
    try:
        value = dbutils.widgets.get(parameter_name)
    except:  # If parameter does not exist
        value = default_value

    return value
