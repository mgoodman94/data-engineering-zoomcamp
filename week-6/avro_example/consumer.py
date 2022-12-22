""" AVRO CONSUMER DEMO """
import logging
from confluent_kafka.avro import AvroConsumer

logging.basicConfig(level=logging.INFO)


def configure_consumer():
    """
    Configure a kafka consumer
    """
    consumer_config = {"bootstrap.servers": "localhost:9092",
                    "schema.registry.url": "http://localhost:8081",
                    "group.id": "taxirides.avro.consumer.1",
                    "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["yellow_taxi_rides"])
    return consumer


def run_consumer():
    """
    Entrypoint
    """
    consumer = configure_consumer()

    while(True):
        try:
            message = consumer.poll(5)  # check again in 5ms if no messages available
            if message:
                msg  = f"Successfully pulled a record from Topic {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\nmessage key: {message.key()} || message value = {message.value()}"
                logging.info(msg)
                consumer.commit()
        except Exception as error:
            msg = f"Error while trying to pull message: {str(error)}"
            logging.error(msg)
        except KeyboardInterrupt:
            break
    consumer.close()


if __name__ == "__main__":
    run_consumer()
