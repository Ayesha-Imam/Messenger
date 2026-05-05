from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Path
from utils.websocket import ConnectionManager
# from models.message import Message, SendMessageRequest
# from utils.database import messages_collection
from datetime import datetime
import json
from bson import ObjectId
from utils.database import private_messages_collection, group_messages_collection

router = APIRouter()

# get 1 : 1 messages

@router.get("/private/{sender_id}/{receiver_id}")
async def get_private_messages(sender_id: str, receiver_id: str):
    query = {
        "$or": [
            {"sender_id": sender_id, "receiver_id": receiver_id},
            {"sender_id": receiver_id, "receiver_id": sender_id}
        ]
    }
    cursor = private_messages_collection.find(query).sort("timestamp", 1)
    messages = await cursor.to_list(length=None)
    for msg in messages:
        msg["_id"] = str(msg["_id"])
    return messages

# get group messages

@router.get("/group/{group_id}")
async def get_group_messages(group_id: str):
    cursor = group_messages_collection.find({"group_id": group_id}).sort("timestamp", 1)
    messages = await cursor.to_list(length=None)
    for msg in messages:
        msg["_id"] = str(msg["_id"])
    return messages
