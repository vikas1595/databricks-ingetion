from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, lit, struct
from operator import and_, or_
from functools import reduce
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()


class DeltaLakeIncrementalProcessor:
    """
    A class to process delta lake change data feed and append changes to downstream Delta lake table.
    In Medallion architecture, this class can be used to process data from bronze layer to silver layer.
    It automatically dedupes the data based on primary keys and dedupe_column and ensure idempotency.
    It also supports incremental processing of delete events.
    """

    def __init__(
        self,
        upstream_table,
        downstream_table,
        primary_keys,
        dedupe_column,
        checkpoint_location,
        is_delete=False,
    ):
        self.upstream_table = upstream_table
        self.downstream_table = downstream_table
        self.primary_keys = primary_keys
        self.dedupe_column = dedupe_column
        self.checkpoint_location = checkpoint_location
        self.is_delete = is_delete

    @property
    def upstream_table_deltalake(self):
        return DeltaTable.forName(spark, self.upstream_table)

    @property
    def downstream_table_deltalake(self):
        return DeltaTable.forName(spark, self.downstream_table)

    def generate_merge_condition(self):
        return reduce(
            and_,
            [col(f"s.{cl}").eqNullSafe(col(f"t.{cl}")) for cl in self.primary_keys],
        )

    def get_deduped_df(self, df):
        if self.is_delete:
            # use change data feed cols also to dedupe multiple cdf updates
            dedupe_cols = ["_commit_version", "_commit_timestamp"] + (
                self.dedupe_column
            )
            self.dedupe_column = struct(*[col(cl) for cl in dedupe_cols]).alias(
                "dedupe_struct"
            )
            df = df.filter(
                col("_change_type").isin("update_post_image", "insert", "delete")
            )
        else:
            self.dedupe_column = struct(*[col(cl) for cl in self.dedupe_column]).alias(
                "dedupe_struct"
            )

        return (
            df.withColumn(
                "rn",
                row_number().over(
                    Window.partitionBy(self.primary_keys).orderBy(
                        self.dedupe_column.desc()
                    )
                ),
            )
            .filter(col("rn") == lit(1))
            .drop("rn")
        )

    def upsert_deltalake(self, microBatchDF, batch):
        microBatchDF = self.get_deduped_df(microBatchDF)
        (
            self.downstream_table_deltalake.alias("t")
            .merge(microBatchDF.alias("s"), condition=self.generate_merge_condition())
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def upsert_delete_deltalake(self, microBatchDF, batch):
        microBatchDF = self.get_deduped_df(microBatchDF)
        (
            self.downstream_table_deltalake.alias("t")
            .merge(microBatchDF.alias("s"), condition=self.generate_merge_condition())
            .whenMatchedUpdateAll(condition=col("_change_type") != "delete")
            .whenNotMatchedInsertAll()
            .whenMatchedDelete(condition=col("_change_type") == "delete")
            .execute()
        )

    def generate_readstream_query(self):
        if not self.is_delete:
            return spark.readStream.option("ignoreChanges", "true").table(
                self.upstream_table
            )
        else:
            return spark.readStream.option("readChangeFeed", "true").table(
                self.upstream_table
            )

    def load_data(self):
        updates = self.generate_readstream_query()
        query = (
            updates.writeStream.foreachBatch(
                self.upsert_deltalake
                if not self.is_delete
                else self.upsert_delete_deltalake
            )
            .option(
                "checkpointLocation",
                self.checkpoint_location,
            )
            .trigger(availableNow=True)
            .outputMode("update")
            .start()
        )
        query.awaitTermination()


# if __name__=='__main__':
#     # Test 1 : upsert merge test
#     deltalake_stream_processor=DeltaLakeIncrementalProcessor(
#         upstream_table="`uc-demo-dev`.raw.orders",
#         downstream_table="`uc-demo-dev`.silver.orders",
#         primary_keys=['order_id'],
#         dedupe_column=['order_timestamp'],
#         checkpoint_location="dbfs:/Volumes/internal_src/files/checkpoints/silver/orders_deltalake",
#         is_delete=False
#     )

#     deltalake_stream_processor.load_data()
#     # Test 2: upsert delete test
#     deltalake_stream_processor_cdf=DeltaLakeIncrementalProcessor(
#         upstream_table="`uc-demo-dev`.raw.orders",
#         downstream_table="`uc-demo-dev`.silver.orders_cdf",
#         primary_keys=['order_id'],
#         dedupe_column=['order_timestamp'],
#         checkpoint_location="dbfs:/Volumes/internal_src/files/checkpoints/silver/orders_deltalake_cdf",
#         is_delete=True
#     )

#     deltalake_stream_processor_cdf.load_data()
