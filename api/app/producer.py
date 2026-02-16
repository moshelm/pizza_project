from confluent_kafka import Producer, Message
import os 
import json

KAFKA = os.getenv("KAFKA",'localhost:9092')
CONFIG = {'bootstrap.servers': KAFKA}

producer = Producer(CONFIG)

def delivery_report(err: Message, msg: Message):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}, {msg.partition()}')

def insert_to_kafka(data:dict):
    producer.produce(topic='pizza-orders',key=data.get("order_id"), value=json.dumps(data.encode('utf-8')), callback=delivery_report)
    producer.poll(0)

def flush():
    producer.flush()