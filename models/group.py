from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import datetime

class GroupCreateRequest(BaseModel):
    name: str = Field(..., min_length=3)
    creator_id: str = Field(..., min_length=1)
    members: List[str] = Field(..., min_items=1)
    group_type: Literal["normal", "broadcast"] = "normal"

class Group(GroupCreateRequest):
    created_at: datetime = Field(default_factory=datetime.now)