# Databricks notebook source
# MAGIC %run ./invoice-streaming
# MAGIC

# COMMAND ----------

class invoicestreamTestSuite():
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
        wc = invoiceStream()
        Squery = wc.process()

        print("Testing first iteration of invoice streams.....")
        self.ingestData(1)
        self.waitforMicroBatch()
        self.assertResult(1253)
        print("First iteration of invoice streams completed.......")

        Squery.stop()


# COMMAND ----------

swcTS = invoicestreamTestSuite()
swcTS.runTest()