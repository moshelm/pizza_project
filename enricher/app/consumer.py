import json 
from confluent_kafka import Consumer
import connections.connection_mongo as mongo
from connections.connection_redis import manager_redis
from bson import ObjectId

def run_logic(consumer: Consumer):
    consumer.subscribe(['cleaned-instructions'])
    while True:
        # polling from kafka
        msg = consumer.poll()
        # error handling
        if msg is None:
            continue
        if msg.error():
            print(f'error in msg {msg.error()}')
            continue
        # convert bytes
        key = msg.key().decode("utf-8")
        data_from_kafka : dict = json.loads(msg.value().decode("utf-8"))

        # fields for analysis
        fields_analysis = {
        'is_kosher':False,
        "is_allergens":False,
        "is_meat":False,
        "is_dairy":True
        }

        # get analysis from redis 
        redis_result = manager_redis.get(f'type:{data_from_kafka['pizza_type']}')

        if redis_result:
            data_hits = redis_result

        else:
            # there is no in cache, create it 
            data_hits = start_logic(data_from_kafka,fields_analysis)
            # send to redis
            manager_redis.setex(f'type:{data_from_kafka['pizza_type']}',20, json.dumps(data_hits))

        # analysis the information
        logic_status(fields_analysis, data_hits)
        status_by_kosher(fields_analysis, data_from_kafka)
        data_from_kafka.update(fields_analysis)

        # update in mongodb
        data_from_kafka['_id'] = ObjectId(data_from_kafka['_id'])
        mongo.collection.replace_one({'order_id':key}, data_from_kafka)
        
def start_logic(order:dict):
    analysis_data = get_data_of_pizza_analysis_lists()
    data_hits = get_hits(analysis_data, order["information"])
    return data_hits
    

def status_by_kosher(fields:dict,order:dict):
    if not fields['is_kosher']:
        order['status'] = 'BURNT'



def get_data_of_pizza_analysis_lists():
    with open('pizza_analysis_lists.json', 'r') as file:
        return json.load(file)
    
def get_hits(data:dict, msg:str):
    return  {"information":{
            "common_allergens":is_hit(data["common_allergens"],msg),
            "meat_ingredients": is_hit(data["meat_ingredients"],msg),
            "dairy_ingredients":is_hit(data["dairy_ingredients"],msg),
            "forbidden_non_kosher":is_hit(data["forbidden_non_kosher"],msg)
            }
            }
    

def logic_status(fields : dict[str:bool], data : dict[str:dict[str:list]]) -> dict[str:list]:
        meat_and_dairy = None

        if data['information']["common_allergens"]:
            fields['is_allergens'] = True
        
        if data['information']["meat_ingredients"]:
            fields['is_meat'] = True
            meat_and_dairy = False

        if data['information']["dairy_ingredients"]:
            fields['is_dairy'] = True
            if meat_and_dairy == False:
                meat_and_dairy = True
            else:
                meat_and_dairy = False

        if not meat_and_dairy or not data['information']["forbidden_non_kosher"]:
            fields['is_kosher'] = True

        
def is_hit(data:list, msg:str):
    hits = []
    for product in data:
        if product in msg:
            hits.append(product)
    return hits
