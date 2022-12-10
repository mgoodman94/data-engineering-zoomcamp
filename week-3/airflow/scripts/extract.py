""" EXTRACT AND SAVE TAXI DATA """
import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

OUTPUT_PATH = os.getenv('OUTPUT_PATH')
ZONE_OUTPUT_PATH = os.getenv('ZONE_OUTPUT_PATH')
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"


def extract_taxi_data():
    """
    extract parqeut file and save as csv
    """
    if not os.path.exists(OUTPUT_PATH):
        log = f"Attempting to extract file: {TAXI_URL}"
        logging.info(log)
        try:
            data = pd.read_parquet(TAXI_URL)
            data.to_csv(OUTPUT_PATH, index=False)
            logging.info("Taxi Extraction Successful")
        except Exception as msg:
            logging.error("Failed to extract the taxi file. Ensure URL exists")
            raise Exception(str(msg))
    else:
        logging.info("Trip File already exists. Moving to next file")

    if not os.path.exists(ZONE_OUTPUT_PATH):
        log = f"Attempting to extract file: {ZONE_URL}"
        logging.info(log)
        try:
            data = pd.read_csv(ZONE_URL)
            data.to_csv(ZONE_OUTPUT_PATH, index=False)
            logging.info("Zone Extraction Successful")
        except Exception as msg:
            logging.error("Failed to extract the zone file. Ensure URL exists")
            raise Exception(str(msg))
    else:
        logging.info("Zone File already exists. Moving to Load")
