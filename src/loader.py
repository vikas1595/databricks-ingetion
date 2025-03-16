from src.spark_session import local_spark_session
from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


class DataLoader:
    def __init__(
        self,
        file_type: str,
        input_file_path: Path,
        schema: StructType,
        target_schema: str,
        target_table_name: str,
        process_mode: str = "append",
    ):
        self.spark = local_spark_session()
        self.file_type = file_type
        self.input_file_path = input_file_path
        self.catalog = "spark_catalog"
        self.target_schema = target_schema
        self.target_table = target_table_name
        self.process_mode = process_mode

    @property
    def full_table_name(self):
        return f"{self.catalog}.{self.target_schema}.{self.target_table}"

    def read_csv(self, header: bool = True, seperator: str = ",")->DataFrame:
        csv_df = (
            self.spark.read.format(self.file_type)
            .options(header=header, sep=seperator)
            .load(self.input_file_path.as_posix())
        )
        return csv_df

    def read_text(self, line_sept="\n", path_glob_filter="txt", recursive=True)->DataFrame:
        text_df = (
            self.spark.read.format("text")
            .options(
                lineSep=line_sept,
                pathGloabFilter=path_glob_filter,
                recursiveFileLookup=recursive,
            )
            .load(self.input_file_path)
        )
        return text_df

    def write_to_table(self, input_df: DataFrame)->None:
        (
            input_df.write.mode(self.process_mode)
            .format("delta")
            .saveAsTable(self.full_table_name)
        )

    def load_data(self):
        match self.file_type:
            case "csv":
                self.write_to_table(self.read_csv())
            case "text":
                self.write_to_table(self.read_text())
            case _:
                raise NotImplementedError
