import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def local_spark_session():
    if SparkSession.getActiveSession():
        return SparkSession.getActiveSession()
    else:
        warehouse_dir = "./temp/warehouse/"
        # Cleanup warehouse directory if it exists
        if os.path.exists(warehouse_dir):
            try:
                shutil.rmtree(warehouse_dir)
                print(f"Deleted existing warehouse directory: {warehouse_dir}")
            except Exception as e:
                print(f"Error deleting warehouse directory: {e}")
        # Create a Spark session with warehouse directory
        builder = (
            SparkSession.builder.appName("DatabricksIngestion")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config(
                "spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0"
            )  # Add Delta dependency
            .config("spark.jars.packages", "io.delta:delta-storage:3.3.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .master("local[2]")
            .appName("local-app")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

def get_spark_session():
    # check databricks runtime"
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.getActiveSession()
    else:
        return local_spark_session()