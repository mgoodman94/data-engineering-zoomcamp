""" AVRO PRODUCER DEMO """
import csv
import logging
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

logging.basicConfig(level=logging.INFO)


def load_avro_schema():
    """
    Load key and value avsc files
    """
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema


def configure_producer():
    """
    Configures the producer to push records to a broker
    """
    key_schema, value_schema = load_avro_schema()
    producer_config = {
        "bootstrap.servers": "localhost:9092", # broker host
        "schema.registry.url": "http://localhost:8081",  # schema registry host
        "acks": "1"  # acknowledge once everything is logged and replicated
    }

    producer = AvroProducer(
        config=producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema
    )

    return producer


def run_producer():
    """
    Entrypoint
    """
    # configure the Producer
    producer = configure_producer()

    # push records to Topic
    with open('ny_taxi_data/taxi_data_500000.csv', 'r') as file:
        csvreader = csv.reader(file)
        next(csvreader, None)  # ignore header
        for row in csvreader:
            key = {"vendorId": int(row[0])}
            value = {
                "vendorId": int(row[0]),
                "passenger_count": int(row[3]),
                "trip_distance": float(row[4]),
                "payment_type": int(row[9]),
                "total_amount": float(row[16])
            }

            try:
                producer.produce(
                    topic="yellow_taxi_rides",
                    key=key,
                    value=value
                )
                msg = f"Successfully produced record: {value}"
                logging.info(msg)
            except Exception as error:
                msg = f"Error while trying to produce record {value}: {str(error)}"
                logging.error(msg)

            producer.flush()


if __name__ == "__main__":
    run_producer()
