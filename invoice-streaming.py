# Databricks notebook source
class invoiceStream():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_invoice"

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

    def readInvoices(self):
        return (spark.readStream
                .format("json")
                .schema(self.getSchema())
                .load(f"{self.base_data_dir}/data/invoices")
                )

    def explodeInvoices(self, invoiceDF):
        return (invoiceDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                     "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                     "DeliveryAddress.State", "DeliveryAddress.PinCode",
                                     "explode(InvoiceLineItems) as LineItem "
                                     )
                )

    def flattenInvoice(self, explodedDF):
        from pyspark.sql.functions import expr
        # flattening the nested JSON structure in the given Source
        return (explodedDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                .withColumn("ItemQty", expr("LineItem.ItemQty"))
                .withColumn("TotalValue", expr("LineItem.TotalValue"))
                .drop("LineItem")

                )

    def appendInvoices(self, flattenedDF):
        return (flattenedDF.writeStream
                .format("delta")
                .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices")
                .outputMode("append")
                .toTable("invoice_line_items")
                )

    def process(self):
        print(f"Starting invoice processing stream.....")
        invoiceDF = self.readInvoices()
        explodedDF = self.explodeInvoices(invoiceDF)
        resultDF = self.flattenInvoice(explodedDF)
        sQuery = self.appendInvoices(resultDF)
        print("Done")
        return sQuery



# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `spark_streaming`; select * from `default`.`invoice_line_items` limit 100;