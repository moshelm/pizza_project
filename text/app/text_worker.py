from confluent_kafka import Consumer
import os 
import connection_mongo as mongo 
import json 
import re

KAFKA = os.getenv("KAFKA","localhost:9092")

CONFIG = {
    'bootstrap.servers': KAFKA, 
     'group.id': 'text_team', 
     'auto.offset.reset': 'earliest'
     }
consumer = Consumer(CONFIG)

def subscribe():
    consumer.subscribe(['pizza-orders'])
    while True:
        msg = consumer.poll()
        if msg is None:
            continue
        if msg.error():
            print(f'error in msg {msg.error()}')
            continue
        key = msg.key().decode("utf-8")
        data = json.loads(msg.value().decode("utf-8"))
        dangers = ["allergy", "peanut", "gluten"]
        for dng in dangers:
            if dng in data["special_instructions"]:
                data['allergies_flaged'] = True
        clean_text = re.sub(r'[^\w\s]','',data["special_instructions"].upper())
        data["cleaned_protocol"] =  clean_text    

        mongo.collection.replace_one(key,data)