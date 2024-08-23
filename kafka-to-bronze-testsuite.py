# Databricks notebook source
# MAGIC %run ./kafka-to-bronze

# COMMAND ----------

class KafkaToBronzeTestSuite():
    def __init__(self):
        # dbutils.fs.mkdirs("dbfs:/FileStore/data_spark_streaming_scholarnest")
        self.base_data_dir = "/FileStore/data_spark_streaming_invoice"

    def cleanTest(self):
        print(f"Starting Cleaning Up.....")
        spark.sql("drop table if exists kafka_bronze_data")
        dbutils.fs.rm("user/hive/warehouse/kafka_bronze_data", True)
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/invoices_bronze", recurse=True)
        print("Cleaning  the tables are done...")

    # def ingestData(self, itr):
    #     print(f"Starting ingestion.....")
    #     dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices")
    #     print("Done......")

    def assertResult(self, expected_count):
        print(f"Starting validation.....")
        spark.sql("select * from kafka_bronze_data limit 10").show()
        actual_count = spark.sql("select count(*) from kafka_bronze_data").collect()[0][0]
        assert expected_count <= actual_count, f"Test failed! acutal count is {actual_count}"
        print("Done")

    def waitforMicroBatch(self):
        import time
        sleep = 80
        print(f"waiting for micro batch to complete.....{sleep}")
        time.sleep(sleep)
        print("Completed")

    def runTest(self):
        self.cleanTest()
        bzstream = Bronze()

        print("Testing Scenario - start from beginning on a new checkpoint.....")
        bzQuery = bzstream.process()
        self.waitforMicroBatch()
        bzQuery.stop()
        self.assertResult(10)
        print("Validation Passed for first case")

        print("Testing Scenario - restart from where it stopped on the checkpoint.....")
        bzQuery = bzstream.process()
        self.waitforMicroBatch()
        bzQuery.stop()
        self.assertResult(10)
        print("Validation Passed for second case")


# COMMAND ----------

ts = KafkaToBronzeTestSuite()
ts.runTest()
