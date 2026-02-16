import redis 
import os

REDIS_HOST = os.getenv('REDIS_HOST','localhost')
REDIS_PORT = int(os.getenv('REDIS_HOST','6379'))

manager_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

