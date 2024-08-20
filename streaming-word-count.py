# Databricks notebook source
base_dir = "dbfs:/FileStore/tables"

# COMMAND ----------

lines = spark.read.format("text").option("lineSep", ".").load(f"{base_dir}/data/text")
display(lines)

# COMMAND ----------

from pyspark.sql.functions import explode,split,trim,lower
raw_words = lines.select(explode(split(lines.value," ")).alias("words"))
quality_words = raw_words.select(lower(trim(raw_words.words))\
.alias("word"))\
.where("word is not null")\
.where("word rlike '[a-z]'")

                                 

# COMMAND ----------

word_count = quality_words.groupBy("word").count()

# COMMAND ----------

display(word_count)

# COMMAND ----------

word_count.write\
.format("delta")\
.mode("overwrite")\
.saveAsTable("word_count_table")

# COMMAND ----------

# spark_streaming.default.word_count_table

# COMMAND ----------

