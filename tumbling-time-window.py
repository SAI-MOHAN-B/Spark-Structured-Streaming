# Databricks notebook source
class TradeSummary():

    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholar"

    def getSchema(self):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        return StructType([StructField("CreatedTime", StringType()), StructField("Type", StringType()),
                           StructField("Amount", DoubleType()), StructField("BrokerCode", StringType())])

    def readBronze(self):
        return spark.readStream.table("kafka_bz")

    def getTrade(self, kafka_df):
        from pyspark.sql.functions import from_json, expr
        # https://stackoverflow.com/questions/62943941/to-date-fails-to-parse-date-in-spark-3-0
        # https://stackoverflow.com/questions/62602720/string-to-date-migration-from-spark-2-0-to-3-0-gives-fail-to-recognize-eee-mmm
        # spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
        # spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        display(kafka_df)
        df = kafka_df.select(from_json(kafka_df.value, self.getSchema()).alias("value"))\
                .select("value.*")\
                .withColumn("CreatedTime", expr("to_timestamp(CreatedTime,'yyyy-MM-dd HH:mm:ss')"))\
                .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))\
                .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
        return df

    def getAggregate(self, trade_df):
        from pyspark.sql.functions import window, sum
        return (trade_df.withWatermark("CreatedTime","30 minutes")
                .groupBy(window(trade_df.CreatedTime, "15 minutes"))
                .agg(sum("Buy").alias("TotalBuy"),
                     sum("Sell").alias("TotalSell"))
                .select("window.start", "window.end", "TotalBuy", "TotalSell")
                )

    def saveResult(self, result_df):
        print("Saving the result into final table")
        return (result_df.writeStream
                .queryName("trade-summary")
                .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/trade_summary")
                .outputMode("complete")
                .toTable("trade_summary")
                )
        print("Done.......saving results")

    def process(self):
        # spark.sql(f"CREATE TABLE kafka_bz(key STRING, value STRING)")
        kafka_df = self.readBronze()
        trade_df = self.getTrade(kafka_df)
        result_df = self.getAggregate(trade_df)
        sQuery = self.saveResult(result_df)
        return sQuery

