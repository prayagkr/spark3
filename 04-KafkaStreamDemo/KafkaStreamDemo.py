import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j
from lib.utils import get_spark_app_config


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark: SparkSession = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # conf_out = spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    logger = Log4j(spark)

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType()),
        ])),
        StructField("InvoiceLineItems", ArrayType(
            StructType([
                StructField("ItemCode", StringType()),
                StructField("ItemDescription", StringType()),
                StructField("ItemPrice", DoubleType()),
                StructField("ItemQty", IntegerType()),
                StructField("TotalValue", DoubleType()),
            ])
        )),
    ])

    kafka_df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice") \
        .option("startingOffsets", "earliest") \
        .load()

    # kafka_df.printSchema()
    value_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("value")
    )

    exploded_df = value_df.selectExpr(
        "value.InvoiceNumber", "value.CreatedTime", "value.PosID", "value.CustomerType",
        "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
        "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode",
        "explode(value.InvoiceLineItems) as LineItem"
    )

    flattened_df = exploded_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.warn("Listening to Kafka")
    invoice_writer_query.awaitTermination()