from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pathlib import Path
from delta.tables import DeltaTable
from src.spark_session import get_spark_session


class MessageFormat:
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"


class JsonOptions:
    def __init__(self, multiLine: bool = True, mode: str = "PERMISSIVE"):
        self.multiLine = multiLine
        self.mode = mode


class CsvOptions:
    def __init__(self, header: bool = False, sep: str = ","):
        self.header = header
        self.sep = sep


def get_reader_options(message_format: MessageFormat, options):
    if message_format == MessageFormat.CSV:
        return CsvOptions(**options).__dict__
    elif message_format == MessageFormat.JSON:
        return JsonOptions(**options).__dict__
    else:
        raise ValueError("Invalid message format")


class FileFormatSpecs:
    def __init__(self, message_format: MessageFormat, options: dict):
        self.message_format = message_format
        self.options = get_reader_options(message_format, options)


class AutoLoader:
    """
    AutoLoader class to load data from raw path to target table
    Args:
        raw_path (str): Raw path to read data from
        file_format_specs (FileFormatSpecs): File format and options
        catalog (str): Catalog name
        database (str): Database name
        target_table_name (str): Target table name
        checkpoint_path (str): Checkpoint path
        autoloader_schema_location (str): Schema location
        expected_message_schema (StructType, optional): Expected message schema. Defaults to None.

    """

    def __init__(
        self,
        raw_path: str,
        file_format_specs: FileFormatSpecs,
        catalog: str,
        database: str,
        target_table_name: str,
        checkpoint_path: str,
        autoloader_schema_location: str,
        expected_message_schema: StructType = None,
        filter_expr: str = None,
    ):
        self.raw_path = raw_path
        self.file_format_specs = file_format_specs
        self.catalog = catalog
        self.database = database
        self.table_name = target_table_name
        self.checkpoint_path = (
            Path(checkpoint_path) / self.database / self.table_name
        ).as_posix()
        self.spark = get_spark_session()
        self.schema_location = (
            Path(autoloader_schema_location) / self.database / self.table_name
        ).as_posix()
        self.query = None
        self.expected_message_schema = expected_message_schema
        self.filter_expr = filter_expr

    @property
    def full_name(self):
        return f"{self.catalog}.{self.database}.{self.table_name}"

    def create_checkpoint_path(self):
        dbutils.fs.mkdirs(self.checkpoint_path)  # noqa: F821

    def delete_checkpoint_path(self):
        dbutils.fs.rm(self.checkpoint_path, recurse=True)  # noqa: F821

    def delete_schema_location(self):
        dbutils.fs.rm(self.schema_location, recurse=True)  # noqa: F821

    def drop_table(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.full_name}")

    def generate_stream_reader(self):
        return (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.file_format_specs.message_format)
            .option("cloudFiles.schemaLocation", self.schema_location)
            .option("cloudFiles.schemaHints", "value binary")
            .option("pathGlobFilter", f"*.{self.file_format_specs.message_format}")
            .option("recursiveFileLookup", "true")
        )

    def add_options(self, df):
        for k, v in self.file_format_specs.options.items():
            df.option(str(k), str(v))
        return df

    def apply_df_operations(self, df):
        if self.expected_message_schema:
            df = df.withColumn(
                "message",
                from_json(col("value").cast("string"), self.expected_message_schema),
            )
        if self.filter_expr:
            df = df.filter(expr(self.filter_expr))
        return df

    def load(self):
        df = self.generate_stream_reader()
        df = self.add_options(df)
        df = df.load(self.raw_path)
        df = self.apply_df_operations(df)

        self.query = (
            df.writeStream.format("delta")
            .option("checkpointLocation", self.checkpoint_path)
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(self.full_name)
            .awaitTermination()
        )

    def stop_query(self):
        if self.query and self.query.isActive:
            self.query.stop()
            print("Streaming query stopped.")
        else:
            print("No active query found.")

if __name__=='__main__':
    message_schema = StructType(
        [
            StructField(
                "books",
                ArrayType(
                    StructType(
                        [
                            StructField("book_id", StringType(), True),
                            StructField("quantity", LongType(), True),
                            StructField("subtotal", LongType(), True),
                        ]
                    )
                ),
            ),
            StructField("customer_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("total", LongType(), True),
        ]
    )

    file_fmt_spcs = FileFormatSpecs(message_format="json", options={"multiLine": "false"})

    dataset_bookstore = "dbfs:/Volumes/internal_src/files/datasets/bookstore"
    checkpoint_path = "dbfs:/Volumes/internal_src/files/datasets"
    checkpoint_path = "dbfs:/Volumes/internal_src/files/checkpoints/"
    data_catalog = "`uc-demo-dev`"
    db_name = "raw"
    autoloader_schema_location = "dbfs://Volumes/internal_src/files/dataset_schema"

    bookstore_al = AutoLoader(
        raw_path=f"{dataset_bookstore}/kafka-raw",
        file_format_specs=file_fmt_spcs,
        catalog=data_catalog,
        database=db_name,
        target_table_name="bookstore",
        checkpoint_path=checkpoint_path,
        autoloader_schema_location=autoloader_schema_location,
        expected_message_schema=message_schema,
        filter_expr="topic='orders'",
    )
    bookstore_al.delete_checkpoint_path()
    bookstore_al.delete_schema_location()
    bookstore_al.drop_table()

    # start streaming
    bookstore_al.load()
    # stop streaming
    bookstore_al.stop_query()
    # check if table is created
    bookstore_al.spark.sql(f"SELECT * FROM {bookstore_al.full_name}").show()
