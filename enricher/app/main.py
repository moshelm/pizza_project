from consumer import run_logic
import os
from confluent_kafka import Consumer

KAFKA = os.getenv("KAFKA","localhost:9092")

CONFIG = {
    'bootstrap.servers': KAFKA, 
     'group.id': 'text_team', 
     'auto.offset.reset': 'earliest'
     }
consumer = Consumer(CONFIG)

run_logic(consumer)