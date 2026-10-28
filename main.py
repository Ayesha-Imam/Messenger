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
from routes.user_routes import router as user_router
from routes.chat_routes import router as chat_router
from routes.group_routes import router as group_router
from routes.doc_editor_routes.collab import router as doc_collab_router
from routes.system_updates_routes.deployment_events import router as event_router


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
# URL: /api/ws/{username}?repo={repository_id}
# Frontend sends username; we resolve to user_id once on connect.
#
# Client → server
#   chat message  {client_id?, content?, file_name?, file_data?, receiver_id: <username> | group_id}
#   mark read     {"type": "mark_read", "chat_with": <username>} | {"type": "mark_read", "group_id": id}
# Server → client
#   "message" / "group_message"  a new message (also echoed to the sender's other tabs)
#   "ack"                        to the sending socket: {client_id, id, timestamp, status: sent|delivered}
#   "delivered_receipt"          {by}: `by` connected and received your pending messages
#   "read_receipt"               {by}: `by` read your messages
#   "conversation_read"          {chat_with | group_id}: you read it in another tab
#   "error"

@app.websocket("/api/ws/{username}")
async def websocket_endpoint(
    websocket: WebSocket,
    username: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    logging.info(f"🔌 WebSocket connection attempt — user: {username}, repo: {repo}")

    await init_db(repo)

    # Resolve username → user_id; connect() rejects and returns None if not found
    user_id = await manager.connect(websocket, username, repo)
    if user_id is None:
        return  # connection was closed inside connect()

    logging.info(f"✅ User '{username}' (id={user_id}) connected via WebSocket.")

    try:
        await _deliver_pending_private_messages(repo, user_id, username)

        while True:
            raw_data = await websocket.receive_text()
            try:
                message_data = json.loads(raw_data)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON."})
                continue
            if not isinstance(message_data, dict):
                await websocket.send_json({"type": "error", "message": "Expected a JSON object."})
                continue

            try:
                if message_data.get("type") == "mark_read":
                    await _mark_read(repo, user_id, username, websocket, message_data)
                else:
                    await _handle_chat_message(repo, user_id, username, websocket, message_data)
            except WebSocketDisconnect:
                raise
            except Exception:
                # Keep the connection alive — one bad message shouldn't drop the user.
                logging.exception(f"❌ Failed to handle message from '{username}' (id={user_id})")
                await websocket.send_json({
                    "type": "error",
                    "client_id": message_data.get("client_id"),
                    "message": "Failed to process message.",
                })

    except WebSocketDisconnect:
        logging.info(f"❌ User '{username}' (id={user_id}) disconnected.")
    finally:
        await manager.disconnect(repo, user_id, websocket)


async def _deliver_pending_private_messages(repo: str, user_id: int, username: str):
    """Messages sent while this user was offline count as delivered once they connect."""
    pool = await get_pool(repo)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT sender_id FROM private_messages WHERE receiver_id = %s AND delivered_at IS NULL",
                (user_id,),
            )
            sender_ids = [row[0] for row in await cur.fetchall()]
            if not sender_ids:
                return
            await cur.execute(
                "UPDATE private_messages SET delivered_at = %s WHERE receiver_id = %s AND delivered_at IS NULL",
                (datetime.now(), user_id),
            )

    receipt = json.dumps({"type": "delivered_receipt", "by": username})
    for sender_id in sender_ids:
        await manager.send_to_user_id(receipt, repo, sender_id)


async def _mark_read(repo: str, user_id: int, username: str, websocket: WebSocket, data: dict):
    chat_with = data.get("chat_with")
    group_id = data.get("group_id")
    pool = await get_pool(repo)
    now = datetime.now()

    if chat_with:
        other_id = await manager.resolve_username_to_id(chat_with, repo)
        if other_id is None:
            return
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE private_messages
                    SET    read_at = %s, delivered_at = COALESCE(delivered_at, %s)
                    WHERE  sender_id = %s AND receiver_id = %s AND read_at IS NULL
                    """,
                    (now, now, other_id, user_id),
                )
                updated = cur.rowcount
        if updated:
            await manager.send_to_user_id(
                json.dumps({"type": "read_receipt", "by": username}), repo, other_id
            )
            await manager.send_to_user_id(
                json.dumps({"type": "conversation_read", "chat_with": chat_with}),
                repo, user_id, exclude=websocket,
            )

    elif group_id:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE group_members
                    SET    last_read_message_id = (
                               SELECT COALESCE(MAX(id), 0) FROM group_messages WHERE group_id = %s)
                    WHERE  group_id = %s AND member_id = %s
                    """,
                    (group_id, group_id, user_id),
                )
                updated = cur.rowcount
        if updated:
            await manager.send_to_user_id(
                json.dumps({"type": "conversation_read", "group_id": group_id}),
                repo, user_id, exclude=websocket,
            )


async def _handle_chat_message(repo: str, user_id: int, username: str, websocket: WebSocket, message_data: dict):
    client_id   = message_data.get("client_id")   # echoed back in the ack
    file_name   = message_data.get("file_name")
    file_data   = message_data.get("file_data")   # base64 encoded
    content     = message_data.get("content")
    # Frontend still sends usernames here — we resolve them below
    receiver_username = message_data.get("receiver_id")
    group_id          = message_data.get("group_id")

    logging.info(f"📨 Message from '{username}' (id={user_id})")

    async def send_error(message: str):
        await websocket.send_json({"type": "error", "client_id": client_id, "message": message})

    # Validate
    if (not content and not file_name):
        await send_error("Message must have either content or a file.")
        return

    target_count = (1 if receiver_username else 0) + (1 if group_id else 0)
    if target_count == 0:
        await send_error("Either 'receiver_id' or 'group_id' must be provided.")
        return
    elif target_count == 2:
        await send_error("Cannot provide both 'receiver_id' and 'group_id'.")
        return

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
            await send_error("Failed to process uploaded file.")
            return

    file_url = f"/api/files/{file_name}" if file_name else None
    now  = datetime.now()
    pool = await get_pool(repo)

    # ── Private message ───────────────────────────────────────────────────
    if receiver_username:
        # Resolve receiver username → id
        receiver_id = await manager.resolve_username_to_id(receiver_username, repo)
        if receiver_id is None:
            await send_error(f"User '{receiver_username}' not found.")
            return

        logging.info(f"📤 Private: '{username}' (id={user_id}) → '{receiver_username}' (id={receiver_id})")

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
                message_id = cur.lastrowid

        payload = {
            "type": "message",
            "id": message_id,
            "sender_id": user_id,
            "sender_username": username,
            "receiver_username": receiver_username,
            "content": content,
            "file_name": file_name,
            "file_url": file_url,
            "timestamp": now.isoformat(),
        }
        delivered = await manager.send_to_user_id(json.dumps(payload), repo, receiver_id)
        status = "delivered" if delivered else "sent"
        if delivered:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE private_messages SET delivered_at = %s WHERE id = %s",
                        (now, message_id),
                    )
        logging.info(
            f"{'✅ Delivered' if delivered else '⚠️ Offline — saved'} private message to '{receiver_username}' (id={receiver_id})"
        )

        # Sender's other tabs show it too
        await manager.send_to_user_id(
            json.dumps({**payload, "status": status}), repo, user_id, exclude=websocket
        )
        await websocket.send_json({
            "type": "ack",
            "client_id": client_id,
            "id": message_id,
            "timestamp": now.isoformat(),
            "status": status,
        })

    # ── Group message ─────────────────────────────────────────────────────
    elif group_id:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:

                await cur.execute(
                    "SELECT id, group_type, creator_id FROM messenger_groups WHERE id = %s",
                    (group_id,),
                )
                group = await cur.fetchone()

                if not group:
                    await send_error("Group not found.")
                    logging.info(f"❌ Group {group_id} not found")
                    return

                if group["group_type"] == "broadcast" and user_id != group["creator_id"]:
                    await send_error("Only the creator can send messages in a broadcast group.")
                    logging.info(f"⚠️ Unauthorised broadcast by '{username}'")
                    return

                await cur.execute(
                    """
                    INSERT INTO group_messages
                        (sender_id, group_id, content, file_name, file_url, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, group["id"], content, file_name, file_url, now),
                )
                message_id = cur.lastrowid

                await cur.execute(
                    "SELECT member_id FROM group_members WHERE group_id = %s",
                    (group["id"],),
                )
                member_rows = await cur.fetchall()

        payload = json.dumps({
            "type": "group_message",
            "id": message_id,
            "group_id": group["id"],
            "sender_id": user_id,
            "sender_username": username,
            "content": content,
            "file_name": file_name,
            "file_url": file_url,
            "timestamp": now.isoformat(),
        })
        for row in member_rows:
            member_id = row["member_id"]
            # The sender's own other tabs get it too, just not the socket it came from.
            delivered = await manager.send_to_user_id(
                payload, repo, member_id, exclude=websocket if member_id == user_id else None
            )
            if member_id != user_id:
                logging.info(
                    f"{'✅ Delivered' if delivered else '⚠️ Offline — saved'} group message to id={member_id}"
                )

        await websocket.send_json({
            "type": "ack",
            "client_id": client_id,
            "id": message_id,
            "timestamp": now.isoformat(),
            "status": "sent",
        })


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health_check(repo: str = Query(..., description="Repository ID to test")):
    try:
        pool = await get_pool(repo)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        return {
            "status": "ok",
            "database": f"{repo}_EEATool",
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
app.include_router(doc_collab_router)  # websocket router for Document Editor module
app.include_router(event_router)

app.mount("/api/files", StaticFiles(directory="uploaded_files"), name="files")