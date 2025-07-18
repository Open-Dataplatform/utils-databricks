#########################################################################
# This module servers to produce test data, which the module can be be 
# tested against. The data files should be uploaded to 
# dplandingtest/custom_utils_test_data 

from datetime import datetime, timedelta, date
from pathlib import Path
import random
from json import dumps, JSONEncoder
from typing import Any
from uuid import UUID
from time import sleep
#from dicttoxml import dicttoxml
from xml.etree.ElementTree import ElementTree, fromstring, tostring
import pandas as pd
from xml.dom.minidom import parseString
from .schema_strings import _get_schema_string
class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            """Maps default of date to datetime

            :param object obj: Object to make serializeable

            :returns object: Serializable object
            """
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()

def _to_xml(data: dict[str, Any] | list[Any] | Any, root: str = "data") -> str:
    """Turns object to xml string

    Args:
        data (dict[str, Any]): Data to convert

    Returns:
        str: xml formatted data str
    """
    xml = f'<{root}> \n'
    if isinstance(data, dict):
        for key, value in data.items():
            xml += _to_xml(value, key)
    elif isinstance(data, list):
        for item in data:
            xml += _to_xml(item, 'item')
    else:
        xml += f"{data} \n"
    xml += f'</{root} \n>'
    #xml: str = dicttoxml({"data": data}, attr_type=False, custom_root=root)
    return xml #parseString(xml).toprettyxml() #xml_data.to_xml()#parseString(xml).toprettyxml()

def _generate_data(n: int =100, seed: int = 42, include_date_time: bool = True, dec: int = 3) -> list[dict[str, Any]]:
    '''
    Generates data in a dict object
    
    :param int n: Number of datapoints, defaults to 100.
    :param int seed: random seed, defaults to 42.
    :param bool include_date_time: if true includes datetime in the data, defaults to True
    :param int dec: number of decimals in the random number generator, defaults to 3
    
    :return dict data: created data in dict object.
    '''
    random.seed(seed)
    ingredients: list[str] = ['flour', 'egg', 'oil', 'milk', 'water', 'salt', 'sugar']
    enum_str: list[str] = [f'str_{i}' for i in range(10)]
    start_date: datetime = datetime(2023,1,1)
    data: list[dict[str, Any]] = []
    for _ in range(n):
        row = {
            'A' : round(random.gauss(mu=0, sigma=1), dec),
            'B' : round(random.uniform(a=0, b=1), dec),
            'C' : round(random.randint(a=0, b=100), dec),
            'D' : round(random.expovariate(lambd=1), dec),
            'E' : random.choice(ingredients),
            'F' : random.choice(enum_str),
            }
        if include_date_time:
            row['G'] = start_date + timedelta(days=_+1)
        row['H'] = {'id': str(UUID(bytes=bytes(random.getrandbits(8) for _ in range(16)), version=4)),
                    'value': random.choice(range(10))}
        data.append(row)
    return data

def get_schema(format: str = "json"):
    """Returns schema string for either json or xml.
    """
    return _get_schema_string(format=format)

def _format_data(data: list[dict[str, Any]], format) -> str:
    if format == "xml":
        return _to_xml(data)
    return dumps({"data": data}, indent=4, cls=DateTimeEncoder)
    
def generate_files(data_dump_dir: Path, n_files: int = 2, n_data_points: int = 3, include_duplicates: bool = True, format: str = "json"):
    """Generates json data files

    Args:
        data_dump_dir (Path): Path to directory to dump data.
        n_files (int, optional): number of files to generate. Defaults to 2.
        n_data_points (int, optional): Number of data points per file. Defaults to 3.
        include_duplicates (bool, optional): Whether duplicate rows should be included in other files. Defaults to True.
        format (str, optional): Either xml or json, determines output file of data.
    """
    format = format.lower()
    assert format in ("json", "xml"), f"format should be either xml or json but received {format}"
    schema_suffix: str = "json"
    if format == "xml":
        schema_suffix = "xsd"
    data_dump_dir.mkdir(exist_ok=True, parents=True)
    schema_dir: Path = (data_dump_dir.parent/"schemachecks")/data_dump_dir.name
    schema_dir.mkdir(exist_ok=True, parents=True)
    # 10 is an arbitrary random seed set by AJKKU
    seed_base: int = 10 if n_files <= 10 else n_files
    # Every second file is a duplicate. 2 is an arbitrary choice made by AJKKU
    if include_duplicates:
        seed_base = 2
    random.seed(seed_base)
    for i in range(n_files):
        data: list[dict[str, Any]] = _generate_data(n=n_data_points, seed=i%seed_base)
        formatted_data: str = _format_data(data, format)
        sleep(0.1)
        date_time: datetime = datetime.now()
        
        date_time_ext: str = date_time.strftime("%Y%m%dT%H%M%S"+str(round(date_time.microsecond/10000)).zfill(2))
        uuid_ext: str = str(UUID(bytes=bytes(random.getrandbits(8) for _ in range(16)))).upper()
        
        with open(data_dump_dir/f"custom_utils_test_data_{date_time_ext}_{uuid_ext}.{format}", "w") as f:
            f.write(formatted_data)
    schema: str = get_schema(format=format)
    with open(schema_dir/f"custom_utils_test_data_{format}_schema.{schema_suffix}", "w") as f:
        f.write(schema)    