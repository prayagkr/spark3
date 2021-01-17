import sys

from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == '__main__':
    conf = get_spark_app_config()
    # spark = SparkSession.builder.appName('Hello Spark').master('local[3]').getOrCreate()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    # # To check configuration
    # conf_out = spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    logger = Log4j(spark)
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info('Finished HelloSpark')
    logger.warn('Starting HelloSpark')


    logger.warn('Finished HelloSpark')
    logger.info('Finished HelloSpark')
    spark.stop()
