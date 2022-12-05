""" Load Taxi Data to GCP"""
import logging
from datetime import datetime
from google.cloud import storage, bigquery
import pandas as pd

logging.basicConfig(level=logging.INFO)


class TaxiLoad:
    """
    Taxi ETL class

    :param trip_filepath: taxi trip filepath
    :param bucket: gcp bucket name
    """
    def __init__(self, trip_filepath, bucket, dataset_id):
        self.trip_filepath = trip_filepath
        self.raw_trip_df = None
        self.bucket = bucket
        self.dataset_id = dataset_id
        self.prefix = 'taxi_data'

    def load_data_to_dataframe(self):
        """
        Load csv into dataframe
        """
        self.raw_trip_df = pd.read_csv(self.trip_filepath)

    def transform_data(self, data):
        """
        Convert dataframa columns to datatime for sql load
        """
        data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
        data.tpep_pickup_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
        return data
    
    def load_to_gcp(self):
        """
        Upload data as CSV to GCP bucket
        """
        msg = f"Loading to GCP bucket {self.bucket} has begun..."
        logging.info(msg)

        month = datetime.now().date().month
        new_file = f'ny_taxi_{month}.csv'
        current_files = []
        try:
            client = storage.Client()
            for blob in client.list_blobs(self.bucket, prefix=self.prefix):
                current_files.append(blob.name)
        except Exception as e:
            logging.error(f"Failed to list files in bucket {self.bucket} and prefix {self.prefix}")
            raise Exception(str(e))

        if f"{self.prefix}/{new_file}" not in current_files:
            transformed_df = self.transform_data(self.raw_trip_df)
            try:
                client = storage.Client()
                bucket = client.get_bucket(self.bucket)
                bucket.blob(f"{self.prefix}/{new_file}").upload_from_string(transformed_df.to_csv(index=False), 'text/csv')
                logging.info("Loading successful")
            except Exception as e:
                logging.error(f"Loading to GCP Bucket {self.bucket} has failed")
                raise Exception(str(e))
        else:
            logging.info("File already exists in given prefix")


def ingest_gcp(file, bucket, dataset_id):
    """
    Entrypoint for ny taxi load to gcp

    :param file: trip file name to be extracted
    :param bucket: gcp bucket name
    :param dataset_id: bigquery dataset id
    """
    taxi = TaxiLoad(
        trip_filepath=file,
        bucket=bucket,
        dataset_id=dataset_id
    )
    taxi.load_data_to_dataframe()
    taxi.load_to_gcp()
