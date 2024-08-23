# Databricks notebook source
class Bronze():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
        self.BOOTSTRAP_SERVER = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = ""
        self.CLUSTER_API_SECRET = ""

    def ingestFromKafka(self, startingTime=1):
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required "
                                                  f"username = '{self.CLUSTER_API_KEY}'password = '{self.CLUSTER_API_SECRET}';")
                .option("maxOffsetsPerTrigger", 10)
                .option("subscribe", "kafka-to-bronze")
                .option("startingTime", startingTime)
                .load()
                )

    def getSchema(self):

        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """

    def getInvoices(self, kafka_df):
        from pyspark.sql.functions import from_json
        return (kafka_df.select(kafka_df.key.cast("string").alias("key"),
                                from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"),
                                "topic", "timestamp")
                )

    def process(self, startingTime=1):
        print(f"Starting Bronze Stream...")
        kafka_df = self.ingestFromKafka(startingTime)
        invoices_df = self.getInvoices(kafka_df)
        sQuery = (invoices_df.writeStream
                  .queryName("bronze-kafka-ingestion")
                  .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices_bronze_kafka")
                  .outputMode("append")
                  .toTable("kafka_bronze_data")
                  )

        print("Done")
        return sQuery

