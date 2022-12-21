""" KAFKA PRODUCER DEMO """
import logging
from time import sleep
from json import dumps
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)


def instantiate_producer():
    """
    Instantiates the producer object
    :return: kafka producer object
    """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # broker port
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    return producer

def push_messages(producer, total_messages):
    """
    Uses a producer to send messages to a broker

    :param producer: kafka producer object
    :param total_messages: count of total messages to send to topic
    """
    for i in range(total_messages):
        data = {"number": i}
        producer.send(
            topic='demo_1',
            value=data
        )
        msg = f"Message {i} of {total_messages} sent to topic demo_1"
        logging.info(msg)
        sleep(1)

def run_producer():
    """
    Entrypoint
    """
    max_messages = 2000
    producer = instantiate_producer()
    push_messages(
        producer=producer,
        total_messages=max_messages
    )


if __name__ == "__main__":
    run_producer()
