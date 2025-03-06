from src.loader import BronzeLoader
from pathlib import Path
from src.helper import create_struct_schema, database_schema_setup
from src.spark_session import local_spark_session
from pyspark.sql.types import *

if __name__=='__main__':
    # create database schema if not exist
    database_schema_setup()
    input_file_path=Path("./data/data_group_*.csv")
    expected_schema= [('timestamp','timestamp'),
                      ('turbine_id','int'),
                      ('wind_speed','float'),
                      ('wind_direction','int'),
                      ('power_output','float')
                      ]
    struct_schema=create_struct_schema(expected_schema)
    bronze_loader = BronzeLoader(
        file_type="csv",
        input_file_path=input_file_path,
        schema=struct_schema,
        target_schema='bronze',
        target_table_name='turbine_data',
        process_mode='append'
    )
    bronze_loader.load_data()
