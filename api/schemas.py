from pydantic import BaseModel
from enum import Enum
from typing import Optional


class RequestsFile(BaseModel):
    order_id: str
    pizza_type: str 
    size: str
    quantity: int
    is_delivery: bool
    special_instructions : Optional[str]

# class ResponseMongo(str, Enum):
#      PREPARING =  "preparing"

# class insert_mongo(RequestsFile):
#      status : ResponseMongo