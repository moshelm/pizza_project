from confluent_kafka import Consumer
import os 
import connection_mongo as mongo 
import json 
import re
from bson import ObjectId
from preprocessor import *


KAFKA = os.getenv("KAFKA","localhost:9092")

CONFIG = {
    'bootstrap.servers': KAFKA, 
     'group.id': 'text_team', 
     'auto.offset.reset': 'earliest'
     }
consumer = Consumer(CONFIG)

def subscribe():
    consumer.subscribe(['pizza-orders'])
    try:
        while True:
            msg = consumer.poll()

            # error handling
            if msg is None:
                continue
            if msg.error():
                print(f'error in msg {msg.error()}')
                continue

            # getting data from kafka
            key = msg.key().decode("utf-8")
            data = json.loads(msg.value().decode("utf-8"))
            
            # clean and logic data 
            logic_text_worker(data)
            data_info = get_info_by_pizza_type(data['pizza_type'])
            data_info = clean_data(data_info)
            data['information'] = data_info
            insert_to_kafka(data)
            
    
    finally:
        flush()



def logic_text_worker(data:dict):
    is_danger(data)
    data["cleaned_protocol"] =  clean_data(data["special_instructions"])    
    data['_id']= ObjectId(data['_id'])


def is_danger(data:dict):
    dangers = ["allergy", "peanut", "gluten"]
    for dng in dangers:
        if dng in data["special_instructions"]:
            data['allergies_flaged'] = True

def clean_data(data : str) -> str:
    clean_text = re.sub(r'[^a-zA-Z ]','',data.upper())
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text