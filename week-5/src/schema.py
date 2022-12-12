""" Spark Schema """
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType


zone_schema = StructType() \
            .add("LocationID", IntegerType(), nullable=False) \
            .add("Borough", StringType(), nullable=False) \
            .add("Zone", StringType(), nullable=False) \
            .add("service_zone", StringType(), nullable=False)

trip_schema = StructType() \
            .add("VendorID", IntegerType(), nullable=False) \
            .add("tpep_pickup_datetime", TimestampType(), nullable=True) \
            .add("tpep_dropoff_datetime", TimestampType(), nullable=True) \
            .add("passenger_count", FloatType(), nullable=True) \
            .add("trip_distance", FloatType(), nullable=True) \
            .add("RatecodeID", FloatType(), nullable=True) \
            .add("store_and_fwd_flag", StringType(), nullable=True) \
            .add("PULocationID", IntegerType(), nullable=True) \
            .add("DOLocationID", IntegerType(), nullable=True) \
            .add("payment_type", IntegerType(), nullable=True) \
            .add("fare_amount", FloatType(), nullable=True) \
            .add("extra", FloatType(), nullable=True) \
            .add("mta_tax", FloatType(), nullable=True) \
            .add("tip_amount", FloatType(), nullable=True) \
            .add("tolls_amount", FloatType(), nullable=True) \
            .add("improvement_surcharge", FloatType(), nullable=True) \
            .add("total_amount", FloatType(), nullable=True) \
            .add("congestion_surcharge", FloatType(), nullable=True) \
            .add("airport_fee", FloatType(), nullable=True)
