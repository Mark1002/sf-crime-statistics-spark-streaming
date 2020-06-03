import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
   StructType, StructField, StringType, TimestampType
)
import pyspark.sql.functions as psf


os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell' # noqa

# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', TimestampType(), True),
    StructField('call_date', TimestampType(), True),
    StructField('offense_date', TimestampType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
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
    distinct_table = service_table.select(
        ['original_crime_type_name', 'disposition', 'call_date_time']
    ).withWatermark('call_date_time', '1 minute')

    # count the number of original crime type
    agg_df = distinct_table.groupBy(
        'original_crime_type_name', 'disposition'
    ).count()
    agg_df = agg_df.orderBy(agg_df['count'].desc())

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "data/radio_code.json"
    radio_code_df = spark.read \
        .option("multiline", "true") \
        .json(radio_code_json_filepath)

    # clean up your data so that the column names
    # match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition") # noqa

    # TODO join on disposition column
    join_query = agg_df.join(
        radio_code_df, agg_df["disposition"] == radio_code_df["disposition"]
    ).select("original_crime_type_name", "description", "count")
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    # log4j = spark._jvm.org.apache.log4j
    # log4j.LogManager.getLogger("org").setLevel(log4j.Level.OFF)
    # log4j.LogManager.getLogger("akka").setLevel(log4j.Level.OFF)

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
