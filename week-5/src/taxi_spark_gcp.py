""" Spark Demo """
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from schema import zone_schema, trip_schema
from dotenv import load_dotenv

env_path = Path("..") / ".env"
load_dotenv(dotenv_path=env_path, verbose=True)


class Taxi:
    """
    Taxi class
    """
    def __init__(self, spark):
        self.spark = spark
        self.raw_zone_spark_df = None
        self.raw_trip_spark_df = None
        self.zone_filepath = f"gs://{os.getenv('GCP_BUCKET_NAME')}/zone_data/zones_12.csv"
        self.trip_filepath = f"gs://{os.getenv('GCP_BUCKET_NAME')}/taxi_data/ny_taxi_12.csv"

    def load_df(self):
        """
        Load demo data into spark df
        """
        # zone data
        self.raw_zone_spark_df = self.spark.read \
                        .format("csv") \
                        .option("header", True) \
                        .schema(zone_schema) \
                        .load(self.zone_filepath)

        # trip data
        self.raw_trip_spark_df = self.spark.read \
                        .format("csv") \
                        .option("header", True) \
                        .schema(trip_schema) \
                        .load(self.trip_filepath)


def set_up_gcp_connection():
    """
    Connect to GCP with Spark
    """
    # configure spark
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('GCPTest') \
        .set('spark.jars', os.getenv("GCP_HADOOP_CONNECTOR")) \
        .set('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .set('spark.hadoop.google.cloud.auth.service.account.json.keyfile', os.getenv("GOOGLE_CREDENTIALS"))
    spark_context = SparkContext(conf=conf)

    # specify filesystem type when it sees "gs"
    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", os.getenv("GOOGLE_CREDENTIALS"))
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    # connect
    conn = SparkSession.builder \
        .config(conf=spark_context.getConf()) \
        .getOrCreate()
    return conn


def run_spark():
    """
    Run various methods on sample data in Spark
    """
    spark_conn = set_up_gcp_connection()
    taxi = Taxi(spark=spark_conn)

    # extract
    taxi.load_df()

    # explore
    print(taxi.raw_trip_spark_df.printSchema())
    print(taxi.raw_zone_spark_df.show())
    print(taxi.raw_trip_spark_df.columns)


if __name__ == "__main__":
    run_spark()
