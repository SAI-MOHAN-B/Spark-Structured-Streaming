# Databricks notebook source
# MAGIC %run ./batch-word-count-data
# MAGIC

# COMMAND ----------

class batchWCTestSuite():
    def __init__(self):
        dbutils.fs.mkdirs("dbfs:/FileStore/data_spark_streaming_scholarnest")
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTest(self):
        print(f"Starting Cleaning Up.....")
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/text")

    def ingestData(self, itr):
        print(f"Starting ingestion.....")
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/text/text_data_{itr}.txt", f"{self.base_data_dir}/data/text")
        print("Done......")

    def assertResult(self,expected_count,word_starting):
        print(f"Starting validation.....")
        acutal_count = spark.sql(f"select sum(count) from word_count_table where substr(word,1,1) == '{word_starting}' ").collect()[0][0]
        assert expected_count == acutal_count, f"Test failed! acutal count is {acutal_count}"
        print("Done")

    def runTest(self):
        self.cleanTest()
        wc = batchWC()
        print("Testing first iteration of batch word count.....")
        self.ingestData(1)
        wc.wordCount()
        self.assertResult(37,'s')
        print("First iteration of batch word count completed.......")

        print("Testing second iteration of batch word count.....")
        self.ingestData(2)
        wc.wordCount()
        self.assertResult(34,'a')
        print(f"Second iteration of batch word count completed......")


# COMMAND ----------

bwcTS = batchWCTestSuite()
bwcTS.runTest()