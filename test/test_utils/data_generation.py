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

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            """Maps default of date to datetime

            :param object obj: Object to make serializeable

            :returns object: Serializable object
            """
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()

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

def get_schema():
    """Returns json schema string.
    """
    schema_string: str = '''
{
    "id": "https://example.com/address.schema.json",
    "schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "An address similar to http://microformats.org/wiki/h-card",
    "type": "object",
    "properties": {
        "data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                "A": {
                "type": "number"
                },
                "B": {
                "type": "number"
                },
                "C": {
                "type": "integer"
                },
                "D": {
                "type": "number"
                },
                "E": {
                "type": "string"
                },
                "F": {
                "type": "string"
                },
                "G": {
                "type": "string",
                "format": "date-time"
                },
                "H": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "format": "uuid"
                        },
                        "value": {
                            "type": "integer"
                        }
                    },
                    "required": ["id", "value"]
                }
            },
            "required": [ "A", "B", "C", "D", "E", "F", "G", "H"]
            }
        }
    },
    "required": ["data"]
}'''
    return schema_string
    
    
def generate_files(data_dump_dir: Path, n_files: int = 2, n_data_points: int = 3, include_duplicates: bool = True):
    """Generates json data files

    Args:
        data_dump_dir (Path): Path to directory to dump data.
        n_files (int, optional): number of files to generate. Defaults to 2.
        n_data_points (int, optional): Number of data points per file. Defaults to 3.
        include_duplicates (bool, optional): Whether duplicate rows should be included in other files. Defaults to True.
    """
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
        json_data: str = dumps({"data": data}, indent=4, cls=DateTimeEncoder)
        sleep(0.1)
        date_time: datetime = datetime.now()
        
        date_time_ext: str = date_time.strftime("%Y%m%dT%H%M%S"+str(round(date_time.microsecond/10000)).zfill(2))
        uuid_ext: str = str(UUID(bytes=bytes(random.getrandbits(8) for _ in range(16)))).upper()
        
        with open(data_dump_dir/f"custom_utils_test_data_{date_time_ext}_{uuid_ext}.json", "w") as f:
            f.write(json_data)
    schema: str = get_schema()
    with open(schema_dir/f"custom_utils_test_data_schema.json", "w") as f:
        f.write(schema)    