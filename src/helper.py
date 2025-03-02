from src.spark_session import local_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    IntegerType,
    LongType,
    FloatType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
    BinaryType,
    DoubleType,
)


def database_schema_setup():
    spark = local_spark_session()
    spark.sql(
        "CREATE DATABASE IF NOT EXISTS bronze LOCATION './bronze/'"
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS clean LOCATION './silver/'")


def create_struct_schema(columns: list[tuple[str, str]]) -> StructType:
    str_to_spark_type = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "decimal": DecimalType(10, 2),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "binary": BinaryType(),
    }

    struct_fields = [
        StructField(
            col_name, str_to_spark_type.get(data_type.lower(), StringType()), True
        )
        for col_name, data_type in columns
    ]

    return StructType(struct_fields)
