import pandas as pd
import argparse

from time import time
from sqlalchemy import create_engine
from math import ceil


class TaxiLoad:
    def __init__(self, trip_filepath, zone_filepath, engine):
        self.trip_filepath = trip_filepath
        self.zone_filepath = zone_filepath
        self.engine = engine
        self.file_size = None
        self.chunksize = 100000
        self.raw_trip_df = None
        self.raw_zone_df = None

    def extract_data(self):
        """
        Get file row count and extract data into a dataframe
        """
        self.file_size = sum(1 for row in open(self.trip_filepath, 'r'))
        self.raw_trip_df = pd.read_csv(self.trip_filepath, iterator=True, chunksize=self.chunksize)
        self.raw_zone_df = pd.read_csv(self.zone_filepath)

    def transform_data(self, chunk):
        """
        Convert dataframa columns to datatime for sql load
        """
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        return chunk
    
    def load_zone_sql(self):
        """
        Load taxi zone data into sql using any sql engine
        """
        self.raw_zone_df.to_sql(
            name='taxi_zone',
            con=self.engine,
            if_exists='replace')

    def load_trip_sql(self):
        """
        Load taxi trip data into sql using any sql engine
        """
        total_chunks = ceil(self.file_size / self.chunksize)
        runtime = 0
        for i in range(total_chunks):
            t_start = time()
            chunk = next(self.raw_trip_df)
            transformed_chunk = self.transform_data(chunk)
            transformed_chunk.to_sql(
                name='yellow_taxi_data',
                con=self.engine,
                if_exists='append')
            t_end = time()
            runtime += (t_end - t_start)
            print(f"Chunk {i+1} / {total_chunks} inserted (elapsed time: {round(runtime, 1)} seconds")


def main(trip_file, zone_file):
    """
    Entrypoint for ny taxi ETL

    :param trip_file: trip file name to be extracted
    :param zone_file: zone file name to be extracted
    """
    taxi = TaxiLoad(
        trip_filepath=f"ny_taxi_data/{trip_file}",
        zone_filepath=f"ny_taxi_data/{zone_file}",
        engine = create_engine('postgresql://root:root@postgres:5432/ny_taxi')
    )
    taxi.extract_data()
    taxi.load_zone_sql()
    taxi.load_trip_sql()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--trip_file', help='trip file in ny_taxi_data/ to extract')
    parser.add_argument('--zone_file', help='zone file in ny_taxi_data/ to extract')
    args = parser.parse_args()
    main(args.trip_file, args.zone_file)


