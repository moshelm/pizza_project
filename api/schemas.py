from pydantic import BaseModel

class RequestsFile(BaseModel):
    order_id: str
    pizza_type: str
    size: str
    quantity: int
    is_delivery: bool
    special_instructions:  str