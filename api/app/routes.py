from fastapi import APIRouter,UploadFile,File, HTTPException 
import json
import db.connection_mongo as mongo
from schemas import RequestsFile
from pydantic import ValidationError
from producer import insert_to_kafka, flush
from db.connection_redis import manager_redis
router = APIRouter()

@router.post('/uploadfile',status_code=201)
async def upload_json_file(file: UploadFile = File(...)):
    try: 
        content = await file.read()
        data = json.loads(content)
        for item in data:
            valid_item = RequestsFile(**item).model_dump()
            valid_item['status'] ="PREPARING"
            mongo.collection.insert_one(valid_item)
            insert_to_kafka(valid_item) 
        flush()      

        return {"massage":"success"}
    
    except Exception as e:
        raise HTTPException(status_code=400,detail=f"mongo failed {str(e)}")
    
    except ValidationError as e:
        raise HTTPException(status_code=400,detail=f"not valid file {e.errors()}")

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f'error reading file {str(e)}')
    
@router.get("/order{order_id}",status_code=200)
async def check_in_cache(order_id:str):
    result_redis = manager_redis.get(f'order:{order_id}')
    if result_redis:
        return {"source": "redis_cache",'data':result_redis}
    else:
        result_mongo = mongo.collection.find_one({"order_id":order_id})
        if result_mongo:
            result_mongo['_id']= str(result_mongo['_id'])
            manager_redis.setex(f'order:{order_id}', 60, json.dumps(result_mongo))

            return {"source": "mongodb",'data':result_mongo}
        else:
            raise HTTPException(status_code=400,detail="there is no order id in the system")

