import os 
import json 
from confluent_kafka import Consumer
import connection_mongo as mongo
from connection_redis import manager_redis

KAFKA = os.getenv("KAFKA","localhost:9092")

CONFIG = {
    'bootstrap.servers': KAFKA, 
     'group.id': 'text_team', 
     'auto.offset.reset': 'earliest'
     }
consumer = Consumer(CONFIG)

def subscribe():
    consumer.subscribe(['cleaned-instructions'])
    while True:
        msg = consumer.poll()
        if msg is None:
            continue
        if msg.error():
            print(f'error in msg {msg.error()}')
            continue
        key = msg.key().decode("utf-8")
        data : dict = json.loads(msg.value().decode("utf-8"))
        fields, info = start_logic()
        data.update(fields)
        mongo.collection.replace_one({'order_id':key},data)
        manager_redis.setex(f'type:{data['pizza_type']}',20, json.dumps(info))
        
def start_logic(order:dict) -> tuple[dict,dict]:
    fields = {
        'is_kosher':False,
        "is_allergens":False,
        "is_meat":False,
        "is_dairy":True
        }
    analysis_data = get_data_of_pizza_analysis_lists()
    info = get_hits(fields, analysis_data, order["pizza_info_demo"])
    status_by_kosher(fields, order)
    return fields, info

def status_by_kosher(fields:dict,order:dict):
    if not fields['is_kosher']:
        order['status'] = 'BURNT'



def get_data_of_pizza_analysis_lists():
    with open('pizza_analysis_lists.json', 'r') as file:
        return json.load(file)

def get_hits(fields : dict[str:bool], data : dict[list], msg : str) -> dict[str:list]:
        meat_and_dairy = None
        hits_allergens = is_hit(data["common_allergens"],msg)
        hits_meat = is_hit(data["meat_ingredients"],msg)
        hits_dairy = is_hit(data["dairy_ingredients"],msg)
        hits_kosher = is_hit(data["forbidden_non_kosher"],msg)

        if hits_allergens:
            fields['is_allergens'] = True
        
        if hits_meat:
            fields['is_meat'] = True
            meat_and_dairy = False

        if hits_dairy:
            fields['is_dairy'] = True
            if meat_and_dairy == False:
                meat_and_dairy = True
            else:
                meat_and_dairy = False

        if not meat_and_dairy or not hits_kosher:
            fields['is_kosher'] = True
        
        info = {"information":{
            "common_allergens":hits_allergens,
            "meat_ingredients": hits_meat,
            "dairy_ingredients":hits_dairy,
            "forbidden_non_kosher":hits_kosher
            }
            }
        return info

        
def is_hit(data:list, msg:str):
    hits = []
    for product in data:
        if product in msg:
            hits.append(product)
    return hits
