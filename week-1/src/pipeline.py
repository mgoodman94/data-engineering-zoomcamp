import pandas as pd

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
        self.file_size = sum(1 for row in open(self.filepath, 'r'))
        self.raw_data = pd.read_csv(self.filepath, iterator=True, chunksize=self.chunksize)

    def transform_data(self, chunk):
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        return chunk

    def load_sql(self):
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


def main():
    taxi = TaxiLoad(
        filepath="ny_taxi_data/yellow_tripdata_2022-01.csv",
        engine = create_engine('postgresql://root:root@postgres:5432/ny_taxi')
    )
    taxi.extract_data()
    taxi.load_sql()


if __name__ == "__main__":
    main()


