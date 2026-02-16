from pymongo import MongoClient
import os 

MONGO_HOST = os.getenv('MONGO_HOST','localhost')
MONGO_PORT = os.getenv('MONGO_HOST','27017')
MONGO_DATABASE = os.getenv('MONGO_HOST','db')
MONGO_COLLECTION = os.getenv('MONGO_HOST','orders')

CONFIG = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

client = MongoClient(CONFIG)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]

