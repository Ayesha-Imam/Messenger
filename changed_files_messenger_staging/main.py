import logging
# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from utils.websocket import manager
from datetime import datetime
import json
import base64
import os
from fastapi.staticfiles import StaticFiles
from routes.user_routes import router as user_router
from routes.chat_routes import router as chat_router
from routes.group_routes import router as group_router
from models.message import PrivateMessage, GroupMessage
from utils.database import (
    private_messages_collection,
    group_messages_collection,
    groups_collection,
)
from fastapi.responses import FileResponse
from utils.database import client


app = FastAPI(title="Enterprise Messenger API")

app.add_middleware(
    CORSMiddleware,
    # local
    # allow_origins=[
    #     "http://localhost:3000",
    #     "https://eaxee.com/Eazee2",
    #     "https://eaxee.com",
    # ],
    
    # Staging
    allow_origins=["http://68.183.185.245", "http://68.183.185.245/EaxeeEnterprise"],
   
    # RSG DEV
    # allow_origins=[
    #     "https://web-dev-eaxee.redseaglobal.com",
    #     "http://web-dev-eaxee.redseaglobal.com",
    # ],
    
    # RSG PROD
    # allow_origins=[
    #     "https://eaxee.redseaglobal.com",
    #     "http://eaxee.redseaglobal.com",
    # ],
    
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/files/{file_name}")
async def download_file(file_name: str):
    file_path = f"uploaded_files/{file_name}"
    return FileResponse(
        path=file_path,
        media_type="application/octet-stream",
        filename=file_name,  # triggers download
    )

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    logging.info(f"🔌 WebSocket connection attempt by user {user_id}")
    await manager.connect(websocket, user_id)
    logging.info(f"✅ User {user_id} connected via WebSocket.")
    logging.info(f"✅ Last deployed at Friday- 5:50pm")

    try:
        while True:
            raw_data = await websocket.receive_text()
            message_data = json.loads(raw_data)

            file_name = message_data.get("file_name")
            file_data = message_data.get("file_data")  # base64 encoded

            logging.info(f"📨 Message received from {user_id}: {message_data}")

            content = message_data.get("content")
            sender_id = message_data.get("sender_id")
            receiver_id = message_data.get("receiver_id")
            group_id = message_data.get("group_id")

            if (not content and not file_name) or not sender_id:
                logging.info("❌ Missing content and file, or sender_id")
                await websocket.send_json({
                    "type": "error",
                    "message": "Message must have either content or a file, and sender_id is required."
                })
                continue

            target_count = (1 if receiver_id else 0) + (1 if group_id else 0)
            if target_count == 0:
                logging.info("❌ Neither receiver_id nor group_id provided")
                await websocket.send_json({
                    "type": "error",
                    "message": "Either 'receiver_id' or 'group_id' must be provided."
                })
                continue
            elif target_count == 2:
                logging.info("❌ Both receiver_id and group_id provided")
                await websocket.send_json({
                    "type": "error",
                    "message": "Cannot provide both 'receiver_id' and 'group_id'. Choose one."
                })
                continue

            # Handle file if file_data and file_name are present
            if file_data and file_name:
                try:
                    file_bytes = base64.b64decode(file_data)
                    os.makedirs("uploaded_files", exist_ok=True)
                    file_path = os.path.join("uploaded_files", file_name)
                    with open(file_path, "wb") as f:
                        f.write(file_bytes)
                    logging.info(f"📁 Saved file to: {file_path}")
                except Exception as e:
                    logging.error(f"❌ Failed to save file: {e}")
                    await websocket.send_json({
                        "type": "error",
                        "message": "Failed to process uploaded file."
                    })
                    continue

            # Private Message Handling
            if receiver_id:
                logging.info(f"📤 Sending private message from {user_id} to {receiver_id}")
                file_url = f"/files/{file_name}" if file_name else None
                message = PrivateMessage(
                    sender_id=user_id,
                    receiver_id=receiver_id,
                    content=content,
                    timestamp=datetime.now(),
                    file_name=file_name,
                    file_url=file_url
                )

                await private_messages_collection.insert_one(message.model_dump())

                delivered = await manager.send_personal_message(
                    json.dumps({
                        "type": "message",
                        "sender_id": user_id,
                        "content": content,
                        "file_name": file_name,
                        "file_url": f"/files/{file_name}" if file_name else None
                    }),
                    receiver_id,
                )
                if delivered:
                    logging.info(f"✅ Delivered private message to {receiver_id}")
                else:
                    logging.info(f"⚠️ {receiver_id} is offline — message saved but not delivered in real-time.")

            # Group Message Handling
            elif group_id:
                group = await groups_collection.find_one({"name": group_id})
                if not group:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Group not found."
                    })
                    logging.info(f"❌ Group {group_id} not found")
                    continue

                if group["group_type"] == "broadcast" and user_id != group["creator_id"]:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Only the creator can send messages in a broadcast group."
                    })
                    logging.info(f"⚠️ Unauthorized broadcast by {user_id} to {group_id}")
                    continue

                file_url = f"/files/{file_name}" if file_name else None
                message = GroupMessage(
                    sender_id=user_id,
                    group_id=group_id,
                    content=content,
                    timestamp=datetime.now(),
                    file_name=file_name,
                    file_url=file_url
                )

                await group_messages_collection.insert_one(message.model_dump())

                for member_id in group["members"]:
                    if member_id != user_id:
                        delivered = await manager.send_personal_message(
                            json.dumps({
                                "type": "group_message",
                                "group_id": group_id,
                                "sender_id": user_id,
                                "content": content,
                                "file_name": file_name,
                                "file_url": f"/files/{file_name}" if file_name else None

                            }),
                            member_id,
                        )
                        if delivered:
                            logging.info(f"✅ Delivered group message to {member_id}")
                        else:
                            logging.info(f"⚠️ {member_id} is offline — group message not delivered in real-time.")

    except WebSocketDisconnect:
        await manager.disconnect(user_id)
        logging.info(f"❌ User {user_id} disconnected from WebSocket.")


@app.get("/health")
async def health_check():
    """Health Check: verifies API + DB connectivity."""
    try:
        # Ping the database
        await client.admin.command("ping")
        return {
            "status": "ok",
            "message": "Server is running. Database connected successfully.",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server running, but database connection failed: {str(e)}",
        )


# Routes
app.include_router(user_router, prefix="/api/users")
app.include_router(chat_router, prefix="/api/chat")
app.include_router(group_router, prefix="/api/groups")

# all files saved in uploaded_files/ will be accessible via:
# http://localhost:8000/files/filename.pdf
# or
# https://eaxee.com/files/filename.pdf

app.mount("/files", StaticFiles(directory="uploaded_files"), name="files") 
