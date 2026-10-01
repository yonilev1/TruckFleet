import json
from confluent_kafka import Producer
import pandas as pd
import os
import logging

os.makedirs('logs', exist_ok=True)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/app/logs/log_file.log', filemode='a')

logger = logging.getLogger()


def delivery_callback(err, msg):
    if err:
        logger.error('ERROR: Message failed delivery: {}'.format(err))
    else:
        msg_key = msg.key().decode('utf-8') if msg.key() else "No Key"
        msg_value = msg.value().decode('utf-8') if msg.value() else "No Value"

        logger.info("Produced event to topic {topic}: key = {key} value = {value}".format(
            topic=msg.topic(), key=msg_key, value=msg_value))

def validated_data(row):
    if pd.isna(row[1]) or row[1] is None:
        logger.warning(f"log for event - {row[0]} is missing Truck id")
        return False
    if pd.isna(row[2]) or row[2] is None:
        logger.warning(f"log for truck - {row[1]} is missing timestamp")
        return False
    if pd.isna(row[3]) or row[3] is None:
        logger.warning(f"log for truck - {row[1]} is missing engine_temp")
        return False
    if row[3] < 0:
        logger.warning(f"log for truck - {row[1]} engine_temp cant be negative")
        return False
    return True



def main():
    try:
        config = {'bootstrap.servers': 'kafka:29092'}
        producer = Producer(config)

        topic = "telemetry-events"

        data = pd.read_csv('telemetry_stream.csv').values.tolist()

        for row in data:
            if validated_data(row):
                string_data = json.dumps(row)
                producer.produce(topic, string_data.encode('utf-8'), callback=delivery_callback)
                producer.poll(0)
        producer.flush()
    except Exception as ex:
        logger.exception("got error in producer %s", ex)


if __name__ == "__main__":
    main()