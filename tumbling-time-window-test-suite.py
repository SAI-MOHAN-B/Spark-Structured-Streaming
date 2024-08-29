# Databricks notebook source
# MAGIC %run ./tumbling-time-window

# COMMAND ----------

class TradeSummaryTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholar"

    def cleanTest(self):
        print(f"Starting CleanUp......")
        spark.sql("drop table if exists kafka_bz")
        spark.sql("drop table if exists trade_summary")
        dbutils.fs.rm("/user/hive/warehouse/kafka_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/trade_summary", True)
        spark.sql(f"CREATE TABLE kafka_bz(key String,value String)")

        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/trade_summary", True)
        print("Done.......cleaning")

    def asserttradesummary(self, start, end, expected_buy, expected_sell):
        print("Starting trade Summary validation.....")
        print(f"Query: SELECT TotalBuy, TotalSell FROM trade_summary WHERE date_format({start}, 'yyyy-MM-dd HH:mm:ss') AND date_format({end}, 'yyyy-MM-dd HH:mm:ss')")

        result = (spark.sql(f""" select TotalBuy,TotalSell from trade_summary
                                 where date_format(start,'yyyy-MM-dd HH:mm:ss') = '{start}'
                                 and date_format(end,'yyyy-MM-dd HH:mm:ss')='{end}'
                            """).collect())
        actual_buy = result[0][0]
        actual_sell = result[0][1]
        assert expected_buy == actual_buy, f"Test failed actual buy is {actual_buy}"
        assert expected_sell == actual_sell, f"Test failed ! actual sell is {actual_sell}"
        print("Done...")

    def waitforMicroBatch(self):
        import time
        sleep = 30
        print(f"waiting for micro batch to complete.....{sleep}")
        time.sleep(sleep)
        print("Completed")

    def runTest(self):
        self.cleanTest()
        stream = TradeSummary()
        sQuery = stream.process()
        print("Testing first two events......")
        spark.sql(""" INSERT INTO kafka_bz VALUES('2019-02-05','{"CreatedTime":"2019-02-05 10:05:00","Type":"BUY","Amount":500,"BrokerCode":"ABX"}'),
                                 ('2019-02-05','{"CreatedTime":"2019-02-05 10:12:00","Type":"BUY","Amount":300,"BrokerCode":"ABX"}')  
                                 """)
        self.waitforMicroBatch()
        self.asserttradesummary('2019-02-05 10:00:00', '2019-02-05 10:15:00', 800, 0)

        print("Testing third and  fourth events......")
        spark.sql(""" INSERT INTO kafka_bz VALUES('2019-02-05','{"CreatedTime":"2019-02-05 10:20:00","Type":"BUY","Amount":600,"BrokerCode":"ABX"}'),
                                         ('2019-02-05','{"CreatedTime":"2019-02-05 10:40:00","Type":"BUY","Amount":900,"BrokerCode":"ABX"}')  
                                         """)
        self.waitforMicroBatch()
        self.asserttradesummary('2019-02-05 10:15:00', '2019-02-05 10:30:00', 600, 0)
        self.asserttradesummary('2019-02-05 10:30:00', '2019-02-05 10:45:00', 900, 0)

        print("Testing late events......")
        spark.sql("""INSERT INTO kafka_bz values
        ('2019-02-05','{"CreatedTime":"2019-02-05 10:48:00","Type":"SELL","Amount":500,"BrokerCode":"ABX"}'),
        ('2019-02-05','{"CreatedTime":"2019-02-05 10:25:00","Type":"SELL","Amount":400,"BrokerCode":"ABX"}')
        """)
        self.waitforMicroBatch()
        self.asserttradesummary('2019-02-05 10:45:00','2019-02-05 11:00:00',0,500)
        self.asserttradesummary('2019-02-05 10:15:00','2019-02-05 10:30:00',600,400)
        print("Validation Passed...")
        sQuery.stop()


# COMMAND ----------

trade = TradeSummaryTestSuite()
gz = trade.runTest()

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(end,'HH:mm') as at_time,
# MAGIC sum(TotalBuy) over(order by end) as Buy,
# MAGIC sum(TotalSell) over(order by end) as Sell,
# MAGIC Buy-Sell as Net
# MAGIC from trade_summary
# MAGIC
