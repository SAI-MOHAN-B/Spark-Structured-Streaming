# Databricks notebook source
class streamWC():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (spark.readStream 
                 .format("text") 
                 .option("lineSep", ".") 
                 .load(f"{self.base_data_dir}/data/text"))
        display(lines)
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim,lower
        return (rawDF.select(lower(trim(rawDF.word)).alias("word"))
                     .where("word is not null")
                     .where("word rlike '[a-z]' ")
                )
    def getWordCount(self,qualityDF):
        return qualityDF.groupBy("word").count()
    
    def overwriteWordCount(self,wordCountDF):
        return (
          wordCountDF.writeStream
            .format("delta")
            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/word_count")
            .outputMode("complete")
            .toTable("word_count_table")
        )
    def wordCount(self):
        print(f" Starting Word Count Stream ......")
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        sQuery = self.overwriteWordCount(resultDF)
        print("Completed...")
        return sQuery

