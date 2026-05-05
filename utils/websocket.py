import logging
from typing import Dict
from datetime import datetime

from fastapi import WebSocket
from utils.database import users_collection
from websockets.exceptions import ConnectionClosed


class ConnectionManager:
    def __init__(self):
        # Maps usernames to WebSocket connections
        self.active_sockets: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, username: str):
        await websocket.accept()
        self.active_sockets[username] = websocket
        logging.info(f"User '{username}' connected. Active connections: {list(self.active_sockets.keys())}")

        # Mark user as online in the database
        await users_collection.update_one(
            {"fullName": username},
            {"$set": {
                "is_online": True,
                "last_active": datetime.now()
            }},
            upsert=False
        )

    async def disconnect(self, username: str):
        if username in self.active_sockets:
            del self.active_sockets[username]
            logging.info(f"User '{username}' disconnected. Active connections: {list(self.active_sockets.keys())}")

            # Mark user as offline in the database
            await users_collection.update_one(
                {"fullName": username},
                {"$set": {
                    "is_online": False,
                    "last_active": datetime.utcnow()
                }},
                upsert=False
            )

    async def send_personal_message(self, message: str, username: str) -> bool:
        if username in self.active_sockets:
            logging.info(
                f"Receiver '{username}' is in active sockets. Attempting to send. (Active: {list(self.active_sockets.keys())})"
            )
        else:
            logging.info(
                f"Receiver '{username}' is NOT in active sockets. (Active: {list(self.active_sockets.keys())})"
            )
            return False

        socket = self.active_sockets.get(username)
        if socket:
            try:
                await socket.send_text(message)
                return True
            except (ConnectionClosed, RuntimeError) as e:
                logging.info(f"Connection to '{username}' is stale. Cleaning up. Error: {e}")
                await self.disconnect(username)
                return False
        return False

    async def broadcast(self, message: str):
        logging.info(f"Broadcasting to all active connections: {list(self.active_sockets.keys())}")
        for socket in self.active_sockets.values():
            await socket.send_text(message)

    def get_online_usernames(self):
        return list(self.active_sockets.keys())

manager = ConnectionManager()




# import logging
# from datetime import datetime
# from fastapi import WebSocket
# from utils.database import users_collection
# from utils.redis_client import redis_client
# import asyncio

# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )

# class ConnectionManager:
#     def __init__(self):
#         # Track active WebSocket connections per instance
#         self.connections = {}  # username: WebSocket

#     async def connect(self, websocket: WebSocket, username: str):
#         await websocket.accept()
#         self.connections[username] = websocket

#         # Set user online in DB
#         await users_collection.update_one(
#             {"fullName": username},
#             {"$set": {
#                 "is_online": True,
#                 "last_active": datetime.utcnow()
#             }},
#             upsert=False
#         )

#         # Start Redis listener for this user
#         asyncio.create_task(self.redis_listener(username))

#         logging.info(f"✅ {username} connected. Redis channel listening started.")

#     async def disconnect(self, username: str):
#         if username in self.connections:
#             del self.connections[username]
#             logging.info(f"❌ {username} disconnected")

#             await users_collection.update_one(
#                 {"fullName": username},
#                 {"$set": {
#                     "is_online": False,
#                     "last_active": datetime.utcnow()
#                 }},
#                 upsert=False
#             )

#     async def send_personal_message(self, message: str, recipient_username: str) -> bool:
#         try:
#             await redis_client.publish(recipient_username, message)
#             logging.info(f"📨 Published message to Redis channel: {recipient_username}")
#             return True
#         except Exception as e:
#             logging.error(f"❌ Failed to publish message to {recipient_username}: {e}")
#             return False

#     async def redis_listener(self, username: str):
#         try:
#             pubsub = redis_client.pubsub()
#             await pubsub.subscribe(username)
#             logging.info(f"📡 Subscribed to Redis channel: {username}")

#             async for message in pubsub.listen():
#                 if message is None:
#                     continue
#                 if message["type"] == "message":
#                     text = message["data"]
#                     socket = self.connections.get(username)
#                     if socket:
#                         try:
#                             await socket.send_text(text)
#                             logging.info(f"➡️ Delivered to {username} via WebSocket")
#                         except Exception as e:
#                             logging.warning(f"⚠️ Failed to send to {username}: {e}")
#                             await self.disconnect(username)
#         except asyncio.CancelledError:
#             logging.warning(f"🛑 Redis listener cancelled for {username}")
#         except Exception as e:
#             logging.error(f"🔥 Redis listener crashed for {username}: {e}")

#     def get_online_usernames(self):
#         return list(self.connections.keys())

# manager = ConnectionManager()





# from typing import List
# from fastapi import WebSocket, WebSocketDisconnect

# class ConnectionManager:
#     def __init__(self):
#         self.active_connections: List[dict] = []

#     async def connect(self, websocket: WebSocket, username: str):
#         await websocket.accept()
#         self.active_connections.append({"username": username, "socket": websocket})

#     def disconnect(self, websocket: WebSocket):
#         self.active_connections = [
#             u for u in self.active_connections if u["socket"] != websocket
#         ]

#     async def send_personal_message(self, message: str, websocket: WebSocket):
#         await websocket.send_text(message)

#     async def broadcast(self, message: str):
#         for user in self.active_connections:
#             await user["socket"].send_text(message)

#     def get_online_usernames(self) -> List[str]:
#         return [user["username"] for user in self.active_connections]

# manager = ConnectionManager()