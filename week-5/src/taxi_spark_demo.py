""" Spark Demo """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import zone_schema, trip_schema


class Taxi:
    """
    Taxi class
    """
    def __init__(self, spark):
        self.spark = spark
        self.raw_zone_spark_df = None
        self.raw_trip_spark_df = None
        self.zone_filepath = "ny_taxi_data/taxi+_zone_lookup.csv"
        self.trip_filepath = "ny_taxi_data/yellow_tripdata_2022-01.csv"

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

    def print_head(self):
        """
        Show head of dataframe
        """
        for row in self.raw_zone_spark_df.head(10):
            print(row)

    def run_sql_select(self):
        """
        Run various sql queries on taxi & zone data
        """
        # create temp table so the data is queryable
        self.raw_trip_spark_df.registerTempTable('TripsData')

        self.spark.sql(
            """
            select * from TripsData limit 10;
            """
        ).show()

    def run_sql_join(self):
        """
        Join 2 tables using Spark's join method
        """
        df_join = self.raw_trip_spark_df.join(self.raw_zone_spark_df.withColumn("DOLocationID", F.col("LocationID")), on="DOLocationID")
        print(df_join[["Zone", "fare_amount"]].show())



def run_spark():
    """
    Run various methods on sample data in Spark
    """
    taxi = Taxi(
        spark= SparkSession.builder \
                .master("local[1]") \
                .appName("demo") \
                .getOrCreate()
    )

    # extract
    taxi.load_df()

    # explore
    print(taxi.raw_trip_spark_df.printSchema())
    print(taxi.raw_zone_spark_df.show())
    print(taxi.raw_trip_spark_df.columns)

    # run sql
    taxi.run_sql_select()
    taxi.run_sql_join()


if __name__ == "__main__":
    run_spark()
