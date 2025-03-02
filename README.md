# Databricks Ingestion

This project provides a framework for ingesting data into Databricks using PySpark and Delta Lake. It includes modules for setting up a Spark session, loading data into Delta tables, and creating database schemas.

## Project Structure

```
databricks-ingetion/
│
├── src/
│   ├── spark_session.py
│   ├── loader.py
│   └── helper.py
├── pyproject.toml
└── README.md
```

### Modules

- **spark_session.py**: Contains the function to create a local Spark session configured with Delta Lake.
- **loader.py**: Contains the `BronzeLoader` class for loading data from various file types into Delta tables.
- **helper.py**: Contains helper functions for setting up database schemas and creating Spark `StructType` schemas.

## Usage

### Setting Up the Spark Session

To create a Spark session configured with Delta Lake, use the `local_spark_session` function from `spark_session.py`:

```python
from src.spark_session import local_spark_session

spark = local_spark_session()
```

### Loading Data

To load data into a Delta table, use the `BronzeLoader` class from `loader.py`:

```python
from src.loader import BronzeLoader
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", StringType(), True),
])

loader = BronzeLoader(
    file_type="csv",
    input_file_path=Path("/path/to/input/file.csv"),
    schema=schema,
    target_schema="bronze",
    target_table_name="my_table"
)

loader.load_data()
```

### Setting Up Database Schemas

To set up the database schemas, use the `database_schema_setup` function from `helper.py`:

```python
from src.helper import database_schema_setup

database_schema_setup()
```

### Creating StructType Schemas

To create a `StructType` schema from a list of column names and data types, use the `create_struct_schema` function from `helper.py`:

```python
from src.helper import create_struct_schema

columns = [("column1", "string"), ("column2", "int")]
schema = create_struct_schema(columns)
```

## Requirements

- Python 3.11+
- PySpark
- Delta Lake

## Installation

Install the required packages using Poetry:

```sh
poetry install
```

## License

This project is licensed under the MIT License.