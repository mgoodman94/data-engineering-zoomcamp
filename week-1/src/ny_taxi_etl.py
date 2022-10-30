import pandas as pd
import argparse

from time import time
from sqlalchemy import create_engine
from math import ceil


class TaxiLoad:
    def __init__(self, filepath, engine):
        self.filepath = filepath
        self.engine = engine
        self.file_size = None
        self.chunksize = 100000
        self.raw_data = None
        self.sql_queries = {"create_table": None}

    def extract_data(self):
        """
        Get file row count and extract data into a dataframe
        """
        self.file_size = sum(1 for row in open(self.filepath, 'r'))
        self.raw_data = pd.read_csv(self.filepath, iterator=True, chunksize=self.chunksize)

    def transform_data(self, chunk):
        """
        Convert dataframa columns to datatime for sql load
        """
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        return chunk

    def load_sql(self):
        """
        Load data into sql using any sql engine
        """
        total_chunks = ceil(self.file_size / self.chunksize)
        runtime = 0
        for i in range(total_chunks):
            t_start = time()
            chunk = next(self.raw_data)
            transformed_chunk = self.transform_data(chunk)
            transformed_chunk.to_sql(
                name='yellow_taxi_data',
                con=self.engine,
                if_exists='append')
            t_end = time()
            runtime += (t_end - t_start)
            print(f"Chunk {i+1} / {total_chunks} inserted (elapsed time: {round(runtime, 1)} seconds")


def main(file):
    """
    Entrypoint for ny taxi ETL

    :param file: file name to be extracted
    """
    taxi = TaxiLoad(
        filepath=f"ny_taxi_data/{file}",
        engine = create_engine('postgresql://root:root@postgres:5432/ny_taxi')
    )
    taxi.extract_data()
    taxi.load_sql()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='file in ny_taxi_data/ to extract')
    args = parser.parse_args()
    main(args.file)


