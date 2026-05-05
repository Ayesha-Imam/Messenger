from pydantic import BaseModel

class User(BaseModel):
    username: str
    fullName: str
