# Databricks notebook source
# MAGIC %run ./stream-word-count
# MAGIC

# COMMAND ----------

class streamWCTestSuite():
    def __init__(self):
        # dbutils.fs.mkdirs("dbfs:/FileStore/data_spark_streaming_scholarnest")
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTest(self):
        print(f"Starting Cleaning Up.....")
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/text")

    def ingestData(self, itr):
        print(f"Starting ingestion.....")
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/text/text_data_{itr}.txt", f"{self.base_data_dir}/data/text")
        print("Done......")

    def assertResult(self,expected_count):
        print(f"Starting validation.....")
        spark.sql("select * from word_count_table limit 10").show()
        acutal_count = spark.sql("select sum(count) from word_count_table where substr(word,1,1) == 's'").collect()[0][0]
        assert expected_count == acutal_count, f"Test failed! acutal count is {acutal_count}"
        print("Done")

    def runTest(self):
        import time
        sleep = 60
        self.cleanTest()
        wc = streamWC()
        Squery = wc.wordCount()

        print("Testing first iteration of batch word count.....")
        self.ingestData(1)
        print(f" Waiting for {sleep} seconds........")
        time.sleep(sleep)
        self.assertResult(32)
        print("First iteration of batch word count completed.......")

        print("Testing second iteration of batch word count.....")
        self.ingestData(2)
        print(f" Waiting for {sleep} seconds........")
        time.sleep(sleep)
        self.assertResult(34)
        print(f"Second iteration of batch word count completed......")

        Squery.stop()


# COMMAND ----------

swcTS = streamWCTestSuite()
swcTS.runTest()