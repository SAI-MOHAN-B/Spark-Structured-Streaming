# Databricks notebook source
BOOTSTRAP_SERVER = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
CLUSTER_API_KEY = ""
CLUSTER_API_SECRET = ""

# COMMAND ----------

df = (spark.read.format("kafka")
.option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.sasl.jaas.config", f"{JAAS_MODULE} required "
                                  f"username = '{CLUSTER_API_KEY}'password = '{CLUSTER_API_SECRET}';")
.option("subscribe", "invoices")
.load()
)


# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


