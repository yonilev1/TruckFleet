import json
from confluent_kafka import Consumer, Producer
import pandas as pd
import os
import logging
from pymongo import MongoClient
import redis

os.makedirs('../logs', exist_ok=True)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../logs/log_file.log', filemode='a')

logger = logging.getLogger()

def main():
    config = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'v1',
        'client.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    topic = "telemetry-events"
    consumer.subscribe([topic])

    producer_config = {
        'bootstrap.servers': 'kafka:29092',
        'client.id': 'client2',
    }
    producer = Producer(producer_config)
    producer_topic = "out_of_range"


    uri = "mongodb://root:root@mongo:27017/"
    client = MongoClient(uri)

    redis_db = redis.Redis(host='redis', port=6379, decode_responses=True)

    try:
        database = client.get_database("trucks_data")
        collection = database.get_collection("raw_history")

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"ERROR: {msg.error()}")
            else:
                data = json.loads(msg.value())
                if valid_data(data, redis_db):
                    truck_dock = {
                        'event_id':data[0],
                        'truck_id':data[1],
                        'timestamp':data[2],
                        'engine_temp':data[3]
                    }
                    collection.insert_one(truck_dock)
                    msg_key = msg.key().decode('utf-8') if msg.key() else "No Key"
                    logger.info(f"Consumed and added to mongo: truck_id={data[1]} key={msg_key}")
                else:
                    producer.produce(producer_topic, json.dumps(data).encode('utf-8'), callback=delivery_callback)
                    logger.warning(f"Temp out of range for truck {data[1]}")

    except Exception as ex:
        logger.exception(f"Got exception while consuming: {ex}")
    finally:
        consumer.close()


def valid_data(data, redis_db):
    truck_id = str(data[1])
    current_temp = float(data[3])

    redis_raw = redis_db.get(truck_id)

    if not redis_raw:
        recent_temps = [current_temp]
        redis_db.set(truck_id, json.dumps(recent_temps))
        return True

    recent_temps = json.loads(redis_raw)

    if len(recent_temps) < 10:
        recent_temps.append(current_temp)
        redis_db.set(truck_id, json.dumps(recent_temps))
        return True

    avg_temp = sum(recent_temps) / len(recent_temps)

    if avg_temp <= current_temp * 1.15:
        recent_temps.append(current_temp)
        recent_temps.pop(0)
        redis_db.set(truck_id, json.dumps(recent_temps))
        return True

    return False


def delivery_callback(err, msg):
    if err:
        logger.error('ERROR: Message failed delivery: {}'.format(err))
    else:
        msg_key = msg.key().decode('utf-8') if msg.key() else "No Key"
        msg_value = msg.value().decode('utf-8') if msg.value() else "No Value"

        logger.info("Produced event to topic {topic}: key = {key} value = {value}".format(
            topic=msg.topic(), key=msg_key, value=msg_value))


if __name__ == "__main__":
    main()