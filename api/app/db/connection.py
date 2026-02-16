from pymongo import MongoClient
import os 

MONGO_HOST = os.getenv("MONGO_HOST",'localhost')
MONGO_PORT = int(os.getenv("MONGO_PORT",'27017'))
MONGO_DATABASE = os.getenv("MONGO_DATABASE",'pizza_db')
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION",'orders')

MONGO_CONFIG = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"


client = MongoClient(MONGO_CONFIG)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]


