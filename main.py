import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

import json
import base64
import os
import aiomysql
from datetime import datetime

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from utils.websocket import manager
from utils.database import get_pool, init_db, close_all_pools
from models.message import PrivateMessage, GroupMessage
from routes.user_routes import router as user_router
from routes.chat_routes import router as chat_router
from routes.group_routes import router as group_router


app = FastAPI(title="Enterprise Messenger API")

# ── CORS ──────────────────────────────────────────────────────────────────────

origins = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("shutdown")
async def shutdown():
    await close_all_pools()
    logging.info("🔒 App shutting down. All DB pools closed.")


# ── File download ─────────────────────────────────────────────────────────────

@app.get("/api/files/{file_name}")
async def download_file(file_name: str):
    file_path = f"uploaded_files/{file_name}"
    return FileResponse(
        path=file_path,
        media_type="application/octet-stream",
        filename=file_name,
    )


# ── WebSocket ─────────────────────────────────────────────────────────────────
# URL: /api/ws/{user_id}?repo={repository_id}
# e.g. ws://host/ws/faiza?repo=eaxee_00002e

@app.websocket("/api/ws/{user_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    logging.info(f"🔌 WebSocket connection attempt — user: {user_id}, repo: {repo}")

    # Ensure tables exist for this repo's database (no-op if already done)
    await init_db(repo)

    await manager.connect(websocket, user_id, repo)
    logging.info(f"✅ User {user_id} connected via WebSocket.")

    try:
        while True:
            raw_data = await websocket.receive_text()
            message_data = json.loads(raw_data)

            file_name  = message_data.get("file_name")
            file_data  = message_data.get("file_data")   # base64 encoded
            content    = message_data.get("content")
            sender_id  = message_data.get("sender_id")
            receiver_id = message_data.get("receiver_id")
            group_id   = message_data.get("group_id")

            logging.info(f"📨 Message from {user_id} to {sender_id}")
            # logging.info(f"📨 Message from {user_id}: {message_data}")

            # Validate
            if (not content and not file_name) or not sender_id:
                await websocket.send_json({
                    "type": "error",
                    "message": "Message must have either content or a file, and sender_id is required."
                })
                continue

            target_count = (1 if receiver_id else 0) + (1 if group_id else 0)
            if target_count == 0:
                await websocket.send_json({
                    "type": "error",
                    "message": "Either 'receiver_id' or 'group_id' must be provided."
                })
                continue
            elif target_count == 2:
                await websocket.send_json({
                    "type": "error",
                    "message": "Cannot provide both 'receiver_id' and 'group_id'."
                })
                continue

            # Save file to disk
            if file_data and file_name:
                try:
                    file_bytes = base64.b64decode(file_data)
                    os.makedirs("uploaded_files", exist_ok=True)
                    with open(os.path.join("uploaded_files", file_name), "wb") as f:
                        f.write(file_bytes)
                    logging.info(f"📁 Saved file: {file_name}")
                except Exception as e:
                    logging.error(f"❌ Failed to save file: {e}")
                    await websocket.send_json({
                        "type": "error",
                        "message": "Failed to process uploaded file."
                    })
                    continue

            file_url = f"/api/files/{file_name}" if file_name else None
            now  = datetime.now()
            pool = await get_pool(repo)

            # ── Private message ───────────────────────────────────────────
            if receiver_id:
                logging.info(f"📤 Private: {user_id} → {receiver_id}")

                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            INSERT INTO private_messages
                                (sender_id, receiver_id, content, file_name, file_url, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (user_id, receiver_id, content, file_name, file_url, now),
                        )

                delivered = await manager.send_personal_message(
                    json.dumps({
                        "type": "message",
                        "sender_id": user_id,
                        "content": content,
                        "file_name": file_name,
                        "file_url": file_url,
                    }),
                    receiver_id,
                )
                logging.info(
                    f"{'✅ Delivered' if delivered else '⚠️ Offline — saved'} private message to {receiver_id}"
                )

            # ── Group message ─────────────────────────────────────────────
            elif group_id:
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:

                        await cur.execute(
                            "SELECT id, group_type, creator_id FROM messenger_groups WHERE name = %s",
                            (group_id,),
                        )
                        group = await cur.fetchone()

                        if not group:
                            await websocket.send_json({"type": "error", "message": "Group not found."})
                            logging.info(f"❌ Group {group_id} not found")
                            continue

                        if group["group_type"] == "broadcast" and user_id != group["creator_id"]:
                            await websocket.send_json({
                                "type": "error",
                                "message": "Only the creator can send messages in a broadcast group."
                            })
                            logging.info(f"⚠️ Unauthorised broadcast by {user_id}")
                            continue

                        await cur.execute(
                            """
                            INSERT INTO group_messages
                                (sender_id, group_id, content, file_name, file_url, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (user_id, group_id, content, file_name, file_url, now),
                        )

                        await cur.execute(
                            "SELECT member_id FROM group_members WHERE group_id = %s",
                            (group["id"],),
                        )
                        member_rows = await cur.fetchall()

                for row in member_rows:
                    member_id = row["member_id"]
                    if member_id == user_id:
                        continue
                    delivered = await manager.send_personal_message(
                        json.dumps({
                            "type": "group_message",
                            "group_id": group_id,
                            "sender_id": user_id,
                            "content": content,
                            "file_name": file_name,
                            "file_url": file_url,
                        }),
                        member_id,
                    )
                    logging.info(
                        f"{'✅ Delivered' if delivered else '⚠️ Offline — saved'} group message to {member_id}"
                    )

    except WebSocketDisconnect:
        await manager.disconnect(user_id)
        logging.info(f"❌ User {user_id} disconnected.")


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health_check(repo: str = Query(..., description="Repository ID to test")):
    """Verifies API + DB connectivity for a given repository."""
    try:
        pool = await get_pool(repo)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        return {
            "status": "ok",
            "database": f"{repo}_eeatool",
            "message": "Server is running. Database connected successfully.",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server running, but database connection failed: {str(e)}",
        )


# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(user_router, prefix="/api/users")
app.include_router(chat_router, prefix="/api/chat")
app.include_router(group_router, prefix="/api/groups")

app.mount("/api/files", StaticFiles(directory="uploaded_files"), name="files")