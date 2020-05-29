import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
   StructType, StructField, StringType, IntegerType, TimestampType
)
import pyspark.sql.functions as psf


os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell' # noqa

# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', IntegerType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', TimestampType(), True),
    StructField('call_date', TimestampType(), True),
    StructField('offense_date', TimestampType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', IntegerType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", "sf.crime.stat.topic") \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()

    # TODO select original_crime_type_name and disposition
    distinct_table = ''

    # count the number of original crime type
    agg_df = ''

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = ""
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names
    # match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition") # noqa

    # TODO join on disposition column
    join_query = agg_df \

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
