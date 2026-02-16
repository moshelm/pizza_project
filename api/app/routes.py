from fastapi import APIRouter,UploadFile,File, HTTPException 
import json
import db.connection as mongo
from schemas import RequestsFile
from pydantic import ValidationError


router = APIRouter()

@router.post('/uploadfile',status_code=201)
async def upload_json_file(file: UploadFile = File(...)):
    try: 
        content = await file.read()
        data = json.loads(content)
        valid_data = [RequestsFile(**item).model_dump() | {"status":"PREPARING"} for item in data]

    except ValidationError as e:
        raise HTTPException(status_code=400,detail=f"not valid file {e.errors()}")

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f'error reading file {str(e)}')
    try: 
        mongo.collection.insert_many(valid_data)
        
        return {"massage":"success"}
    except Exception as e:
        raise HTTPException(status_code=400,detail=f"mongo failed {str(e)}")
    
    