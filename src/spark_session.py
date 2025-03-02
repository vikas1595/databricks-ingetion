from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def local_spark_session():
    warehouse_dir = "./temp/warehouse/"
    # Create a Spark session with warehouse directory
    builder = (
        SparkSession.builder.appName("DatabricksIngestion")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.jars.packages","io.delta:delta-spark_2.12:3.3.0")  # Add Delta dependency
        .config("spark.jars.packages","io.delta:delta-storage:3.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[2]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

