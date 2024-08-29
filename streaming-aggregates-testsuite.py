# Databricks notebook source
# MAGIC %run ./streaming-aggregates

# COMMAND ----------

class AggregationTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTests(self):
        print(f"Starting Cleanup......")
        spark.sql("drop table if exists invoices_bz")
        spark.sql("drop table if exists customer_rewards")
        dbutils.fs.rm("/user/hive/warehouse/invoices_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/customer_rewards", True)

        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/customer_rewards", True)

        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")

        print("Done")

    def ingestData(self, itr):
        print("Starting Ingestion.....")
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json",
                      f"{self.base_data_dir}/data/invoices")
        print("Done....")
    
    def assertBronze(self,expected_value):
        print("Starting Bronze....")
        actual_value = spark.sql("select count(*) from invoices_bz").collect()[0][0]
        assert expected_value == actual_value,f"Test failed! actual value is {actual_value}"
        print("Done")

    def assertGold(self, expected_value):
        print("Starting Gold Validation.....")
        actual_value = \
            spark.sql("select TotalAmount from customer_rewards where CustomerCardNo='2262471989'").collect()[0][0]
        assert expected_value == actual_value, f"Test failed! actual value is {actual_value}"
        print("Done.....")

    def waitForMicroBatch(self, sleep=80):
        import time
        print(f"waiting for {sleep} seconds....")
        time.sleep(sleep)
        print("Done.....")

    def runTest(self):
        self.cleanTests()
        bzstream = Bronze()
        bzQuery = bzstream.process()
        gdstream = Gold()
        gdQuery = gdstream.process()

        print("Testing first Iteration of Invoice Stream.....")
        self.ingestData(1)
        self.waitForMicroBatch()
        self.assertBronze(501)
        self.assertGold(36859)
        print("validation passed")

        bzQuery.stop()
        gdQuery.stop()

# COMMAND ----------

aztest = AggregationTestSuite()
aztest.runTest()

# COMMAND ----------

df = spark.read.format("json").option("inferSchema",True).load("dbfs:/FileStore/data_spark_streaming_scholarnest/datasets/invoices/invoices_1.json")
df.show()

# COMMAND ----------


