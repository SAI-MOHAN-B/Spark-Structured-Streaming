# Databricks notebook source
# MAGIC %run ./invoice-stream-trigger

# COMMAND ----------

class invoicestreamTriggerTestSuite():
    def __init__(self):
        # dbutils.fs.mkdirs("dbfs:/FileStore/data_spark_streaming_scholarnest")
        self.base_data_dir = "/FileStore/data_spark_streaming_invoice"

    def cleanTest(self):
        print(f"Starting Cleaning Up.....")
        spark.sql("drop table if exists invoice_line_items")
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")

    def ingestData(self, itr):
        print(f"Starting ingestion.....")
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices")
        print("Done......")

    def assertResult(self,expected_count):
        print(f"Starting validation.....")
        spark.sql("select * from invoice_line_items limit 10").show()
        actual_count = spark.sql("select count(*) from invoice_line_items").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! acutal count is {actual_count}"
        print("Done")

    def waitforMicroBatch(self):
        import time
        sleep = 60
        print(f"waiting for micro batch to complete.....{sleep}")
        time.sleep(sleep)
        print("Completed")

    def runTest(self):
        self.cleanTest()
        wc = invoiceStreamBatch()
        Squery = wc.process("60 seconds")

        print("Testing first iteration of invoice streams.....")
        self.ingestData(1)
        self.waitforMicroBatch()
        self.assertResult(1253)
        print("First iteration of invoice streams completed.......")

        print("Testing second iteration of invoice streams.....")
        self.ingestData(2)
        self.waitforMicroBatch()
        self.assertResult(2510)
        print("second iteration of invoice streams completed.......")


        Squery.stop()
    
    def runBatchTests(self):
        self.cleanTest()
        iStream = invoiceStreamBatch()

        print("Testing first batch of invoice streams.....")
        self.ingestData(1)
        self.ingestData(2)
        iStream.process("batch")
        self.waitforMicroBatch()
        self.assertResult(2506)
        print("first batch of invoice streams completed.......")




# COMMAND ----------

swcTS = invoicestreamTriggerTestSuite()
swcTS.runTest()


# COMMAND ----------

swcTS.runBatchTests()