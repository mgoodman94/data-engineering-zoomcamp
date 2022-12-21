""" KAFKA CONSUMER DEMO """
import logging
from json import loads
from time import sleep
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)


def instantiate_consumer():
    """
    Instantiates the consumer object
    :return: kafka consumer object
    """
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],  # broker port
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer.group.demo.1',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(['demo_1'])
    return consumer

def pull_messages(consumer):
    """
    Uses a consumer to pull messages to a broker

    :param consumer: kafka consumer object
    """
    try:
        for message in consumer:
            if message:
                value = message.value
                msg = f"New message pulled: {value}"
                logging.info(msg)
                sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def run_consumer():
    """
    Entrypoint
    """
    consumer = instantiate_consumer()
    pull_messages(consumer=consumer)


if __name__ == "__main__":
    run_consumer()
