""" Load Taxi Data to GCP"""
import logging
from datetime import datetime
from google.cloud import storage
import pandas as pd

logging.basicConfig(level=logging.INFO)


class TaxiLoad:
    """
    Taxi ETL class

    :param trip_filepath: taxi trip filepath
    :param bucket: gcp bucket name
    :param dataset_id = bigquery dataset id
    """
    def __init__(self, trip_filepath, zone_filepath, bucket, dataset_id):
        self.trip_filepath = trip_filepath
        self.zone_filepath = zone_filepath
        self.raw_trip_df = None
        self.zone_df = None
        self.bucket = bucket
        self.dataset_id = dataset_id

    def load_data_to_dataframe(self):
        """
        Load csv into dataframe
        """
        self.raw_trip_df = pd.read_csv(self.trip_filepath)
        self.zone_df = pd.read_csv(self.zone_filepath)

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
        new_files = {
            'taxi_data': f'ny_taxi_{month}.csv',
            'zone_data': f'zones_{month}.csv'
        }
        current_files = []
        try:
            client = storage.Client()
            for blob in client.list_blobs(self.bucket):
                current_files.append(blob.name)
        except Exception as msg:
            log = f"Failed to list files in bucket {self.bucket}"
            logging.error(log)
            raise Exception(str(msg))

        for prefix, file in new_files.items():
            if f"{prefix}/{file}" not in current_files:
                # create connection to GCP
                try:
                    client = storage.Client()
                    bucket = client.get_bucket(self.bucket)
                except Exception as msg:
                    logging.error("Failed to connect to GCP")
                    raise Exception(str(msg))

                # load any new files
                if prefix == "taxi_data":
                    transformed_df = self.transform_data(self.raw_trip_df)
                    try:
                        bucket.blob(f"{prefix}/{file}").upload_from_string(transformed_df.to_csv(index=False), 'text/csv')
                        logging.info("Loading successful")
                    except Exception as msg:
                        log = f"Loading to GCP prefix {prefix}/{file} has failed"
                        logging.error(log)
                        raise Exception(str(msg))
                elif prefix == "zone_data":
                    try:
                        bucket.blob(f"{prefix}/{file}").upload_from_string(self.zone_df.to_csv(index=False), 'text/csv')
                        logging.info("Loading successful")
                    except Exception as msg:
                        log = f"Loading to GCP prefix {prefix}/{file} has failed"
                        logging.error(log)
                        raise Exception(str(msg))
            else:
                log = f"File already exists in given prefix {prefix}/{file}"
                logging.info(log)


def ingest_gcp(file, zone_file, bucket, dataset_id):
    """
    Entrypoint for ny taxi load to gcp

    :param file: trip file name to be loaded
    :param zone_file: zone file name to be loaded
    :param bucket: gcp bucket name
    :param dataset_id: bigquery dataset id
    """
    taxi = TaxiLoad(
        trip_filepath=file,
        zone_filepath=zone_file,
        bucket=bucket,
        dataset_id=dataset_id
    )
    taxi.load_data_to_dataframe()
    taxi.load_to_gcp()
