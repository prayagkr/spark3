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
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    # kafka_df.printSchema()
    value_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("value")
    )

    notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount", )\
        .withColumn("EarnedLoyaltyPoint", expr("TotalAmount * 0.2"))

    kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                 """to_json(named_struct(
                                                     'CustomerCardNo', CustomerCardNo, 
                                                     'TotalAmount', TotalAmount,
                                                     'EarnedLoyaltyPoint', TotalAmount * 0.2
                                                 )) as value
                                                 """)

    notification_writer_query = kafka_target_df.writeStream \
        .queryName("Notification writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "notifications") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    logger.warn("Listening and writing Kafka")
    notification_writer_query.awaitTermination()
