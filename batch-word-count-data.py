# Databricks notebook source
# dbfs:/FileStore/tables/data/text
dbutils.fs.mkdirs("dbfs:/FileStore/")

# COMMAND ----------

class batchWC():
    def __init__(self):
        self.base_data_dir = "dbfs:/FileStore/tables"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (spark.read \
                 .format("text") \
                 .option("lineSep", ".") \
                 .load(f"{self.base_data_dir}/data/text"))
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim,lower
        return (rawDF.select(lower(trim(rawDF.word)).alias("word"))\
                     .where("word is not null")\
                     .where("word rlike '[a-z]' ")
                )
    def getWordCount(self,qualityDF):
        return qualityDF.groupBy("word").count()
    
    def overwriteWordCount(self,wordCountDF):
        (
          wordCountDF.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("word_count_table")
        )
    def wordCount(self):
        print(f"Executing the word count....")
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.overwriteWordCount(resultDF)
        print("Completed...")



# COMMAND ----------


        

# COMMAND ----------

