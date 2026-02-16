from fastapi import APIRouter,UploadFile,File, HTTPException 
import json
import db.connection as mongo
from schemas import RequestsFile
from pydantic import ValidationError
from producer import insert_to_kafka, flush

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
    
    