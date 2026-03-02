# Kafka Consumer: Reads messages from patient topics.
# Used for verification and as the basis for real-time scoring.


import json
from confluent_kafka import Consumer

from src.utils.config import config
from src.utils.logging_config import setup_logging

log = setup_logging("kafka_consumer", json_output=False)


def consume_vitals(max_messages=10):
    consumer = Consumer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "group.id": "sepsis-verify",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([config.kafka.vitals_topic])

    count = 0
    try:
        while count < max_messages:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                log.error("Consumer error", error=msg.error())
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"  [{data['vital_name']}] stay={data['stay_id']} "
                  f"value={data['valuenum']} time={data['charttime']}")
            count += 1

    finally:
        consumer.close()

    print(f"\nConsumed {count} messages from {config.kafka.vitals_topic}")


if __name__ == "__main__":
    consume_vitals(max_messages=10)
