from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class PrivateMessage(BaseModel):
    sender_id: str
    receiver_id: str
    content: Optional[str] = None
    timestamp: datetime
    file_name: Optional[str] = None
    file_url: Optional[str] = None


class GroupMessage(BaseModel):
    sender_id: str
    group_id: str
    content: Optional[str] = None
    timestamp: datetime
    file_name: Optional[str] = None
    file_url: Optional[str] = None


class SendMessageRequest(BaseModel):
    sender_id: str
    receiver_id: str
    content: str