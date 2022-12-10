""" Load Taxi Data to Postgres """
from math import ceil
from time import perf_counter
import logging
from sqlalchemy import create_engine
import pandas as pd

logging.basicConfig(level=logging.INFO)


class TaxiLoad:
    """
    Taxi ETL class

    :param trip_filepath: taxi trip filepath
    :param zone_filepath: taxi zone lookup filepath
    :param engine: sql engine object
    :param table_name: taxi table name
    :param zone_table_name: zone table name
    """
    def __init__(self, trip_filepath, zone_filepath, engine, table_name, zone_table_name):
        self.trip_filepath = trip_filepath
        self.zone_filepath = zone_filepath
        self.engine = engine
        self.file_size = None
        self.chunksize = 100000
        self.raw_trip_df = None
        self.table_name = table_name
        self.zone_table_name = zone_table_name

    def load_data_to_dataframe(self):
        """
        Get file row count and extract data into a dataframe
        """
        self.file_size = sum(1 for row in open(self.trip_filepath, 'r', encoding="utf8"))
        self.raw_trip_df = pd.read_csv(self.trip_filepath, iterator=True, chunksize=self.chunksize)

    def transform_data(self, chunk):
        """
        Convert dataframa columns to datatime for sql load
        """
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        return chunk

    def load_trip_sql(self):
        """
        Load taxi trip data into sql using any sql engine
        """
        total_chunks = ceil(self.file_size / self.chunksize)
        runtime = 0
        log = f"Loading to {self.table_name} has begun..."
        logging.info(log)
        for i in range(total_chunks):
            t_start = perf_counter()
            chunk = next(self.raw_trip_df)
            transformed_chunk = self.transform_data(chunk)
            try:
                transformed_chunk.to_sql(
                    name=self.table_name,
                    con=self.engine,
                    if_exists='append')
            except Exception as msg:
                log = f"Load to table failed, continuing to next chunk. Reason: {str(msg)}"
                logging.warning(log)
                continue
            t_end = perf_counter()
            runtime += (t_end - t_start)
            msg = f"Chunk {i+1} / {total_chunks} inserted \
                (elapsed time: {round(runtime, 1)} seconds"
            logging.info(msg)

    def load_zone_data(self):
        """
        Load zone lookup data
        """
        zone_df = pd.read_csv(self.zone_filepath)
        try:
            zone_df.to_sql(
                name=self.zone_table_name,
                con=self.engine,
                if_exists='replace'
            )
            logging.info("Zone extract was successful.")
        except Exception as msg:
            logging.error("Zone load failed")
            raise Exception(str(msg))



def ingest_postgres(user, password, host, port, database, table_name, zone_table_name, file, zone_file):
    """
    Entrypoint for ny taxi load to postgres

    :param user: postgres user
    :param password: postgres password
    :param host: engine host
    :param port: postgres port
    :param database: postgres database
    :param table_name: taxi postgres table
    :param zone_table_name: taxi zone table
    :param file: trip file name to be loaded
    :param zone_file: zone file name to be loaded
    :param bucket: gcp bucket name
    """
    taxi = TaxiLoad(
        trip_filepath=file,
        zone_filepath=zone_file,
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}'),
        table_name=table_name,
        zone_table_name=zone_table_name
    )
    taxi.load_data_to_dataframe()
    taxi.load_trip_sql()
    taxi.load_zone_data()
