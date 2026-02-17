import json
import os
from confluent_kafka import Producer, Message

KAFKA = os.getenv("KAFKA",'localhost:9092')
CONFIG = {'bootstrap.servers': KAFKA}

producer = Producer(CONFIG)

def delivery_report(err: Message, msg: Message):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}, {msg.partition()}')

def insert_to_kafka(data:dict):
    producer.produce(topic='cleaned-instructions',key=data["order_id"].encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.poll(0)

def flush():
    producer.flush()


def get_info_by_pizza_type(pizza_type: str) -> str | None:
    with open('pizza_prep.json','r') as file:
        data : dict = json.load(file)
        for key in  data.keys():
            if key == pizza_type:
                return data[key]
        return None

