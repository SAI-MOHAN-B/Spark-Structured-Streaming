# Databricks notebook source
# clean and setup
base_data_dir = "dbfs:/FileStore/data_spark_streaming_invoice"

# COMMAND ----------

# spark_streaming.default.word_count_table
spark.sql(""" drop table if exists word_count_table """)

# COMMAND ----------

dbutils.fs.mkdirs(f"{base_data_dir}/datasets/invoices")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/data/text")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/data_spark_streaming_scholarnest/datasets/text")

# COMMAND ----------

dbutils.fs.rm("/FileStore/data_spark_streaming_invoice/checkpoint",recurse=True)

# COMMAND ----------

