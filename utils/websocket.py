import logging
from typing import Dict
from datetime import datetime

from fastapi import WebSocket
from websockets.exceptions import ConnectionClosed


class ConnectionManager:
    def __init__(self):
        # { username: WebSocket }
        self.active_sockets: Dict[str, WebSocket] = {}
        # { username: repository_id }  — needed for DB updates on disconnect
        self.user_repo: Dict[str, str] = {}

    async def connect(self, websocket: WebSocket, username: str, repository_id: str):
        await websocket.accept()
        self.active_sockets[username] = websocket
        self.user_repo[username] = repository_id
        logging.info(
            f"User '{username}' connected (repo: {repository_id}). "
            f"Active: {list(self.active_sockets.keys())}"
        )

        from utils.database import get_pool
        try:
            pool = await get_pool(repository_id)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE users
                        SET    status = '1'
                        WHERE  username = %s
                        """,
                        (username),
                    )
        except Exception as e:
            logging.warning(f"⚠️ Could not update online status for '{username}': {e}")

    async def disconnect(self, username: str):
        repository_id = self.user_repo.pop(username, None)

        if username in self.active_sockets:
            del self.active_sockets[username]
            logging.info(
                f"User '{username}' disconnected. Active: {list(self.active_sockets.keys())}"
            )

        if repository_id:
            from utils.database import get_pool
            try:
                pool = await get_pool(repository_id)
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE users
                            SET    status = '0'
                            WHERE  username = %s
                            """,
                            (username),
                        )
            except Exception as e:
                logging.warning(f"⚠️ Could not update offline status for '{username}': {e}")

    async def send_personal_message(self, message: str, username: str) -> bool:
        if username not in self.active_sockets:
            logging.info(
                f"Receiver '{username}' NOT in active sockets. "
                f"Active: {list(self.active_sockets.keys())}"
            )
            return False

        logging.info(f"Sending to '{username}'...")
        socket = self.active_sockets[username]
        try:
            await socket.send_text(message)
            return True
        except (ConnectionClosed, RuntimeError) as e:
            logging.info(f"Stale connection for '{username}', cleaning up. Error: {e}")
            await self.disconnect(username)
            return False

    async def broadcast(self, message: str):
        for socket in self.active_sockets.values():
            await socket.send_text(message)

    def get_online_usernames(self):
        return list(self.active_sockets.keys())


manager = ConnectionManager()