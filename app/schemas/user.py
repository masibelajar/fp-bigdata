from pydantic import BaseModel

class UserCreate(BaseModel):
    name: str
    email: str

class UserOut(UserCreate):
    id: int