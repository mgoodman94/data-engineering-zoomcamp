""" EXTRACT AND SAVE TAXI DATA """
import os
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)

DATA_URL_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
DATA_URL_SUFFIX = f"yellow_tripdata_{datetime.now().strftime('%Y-%m')}.parquet"
OUTPUT_PATH = os.getenv('OUTPUT_PATH')
TEST_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"

def extract_taxi_data():
    """
    extract parqeut file and save as csv
    """
    if not os.path.exists(OUTPUT_PATH):
        logging.info(f"Attempting to extract file: {TEST_URL}")
        try:
            data = pd.read_parquet(TEST_URL)
            data.to_csv(OUTPUT_PATH)
            logging.info("Extraction Successful")
        except Exception as e:
            logging.warning(f"Exception: {str(e)}")
            logging.warning("Failed to extract the file. Ensure URL exists")
    else:
        logging.info("File already exists. Moving to Load")