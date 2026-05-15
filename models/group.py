from pydantic import BaseModel, Field
from typing import List, Literal, Optional
from datetime import datetime


class GroupCreateRequest(BaseModel):
    name: str = Field(..., min_length=3)
    creator_id: str = Field(..., min_length=1)
    members: List[str] = Field(..., min_length=1)
    group_type: Literal["normal", "broadcast"] = "normal"


class Group(GroupCreateRequest):
    id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)