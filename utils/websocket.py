import logging
from typing import Dict, Optional
from fastapi import WebSocket
from websockets.exceptions import ConnectionClosed


class ConnectionManager:
    def __init__(self):
        # { user_id (int): WebSocket }
        self.active_sockets: Dict[int, WebSocket] = {}
        # { user_id (int): repository_id } — needed for DB updates on disconnect
        self.user_repo: Dict[int, str] = {}
        # { username (str): user_id (int) } — for reverse lookup when sending
        self.username_to_id: Dict[str, int] = {}

    async def resolve_user_id(self, username: str, repository_id: str) -> Optional[int]:
        """Look up the integer user ID for a given username."""
        from utils.database import get_pool
        pool = await get_pool(repository_id)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM users WHERE username = %s AND enabled = '1' LIMIT 1",
                    (username,),
                )
                row = await cur.fetchone()
                return row[0] if row else None

    async def connect(self, websocket: WebSocket, username: str, repository_id: str) -> Optional[int]:
        """
        Accept the WebSocket, resolve username → user_id, update status.
        Returns the resolved user_id, or None if the user wasn't found.
        """
        user_id = await self.resolve_user_id(username, repository_id)

        if user_id is None:
            logging.warning(f"⚠️ Username '{username}' not found in repo '{repository_id}'. Rejecting connection.")
            await websocket.close(code=4001, reason="User not found")
            return None

        await websocket.accept()
        self.active_sockets[user_id] = websocket
        self.user_repo[user_id] = repository_id
        self.username_to_id[username] = user_id

        logging.info(
            f"User '{username}' (id={user_id}) connected (repo: {repository_id}). "
            f"Active IDs: {list(self.active_sockets.keys())}"
        )

        from utils.database import get_pool
        try:
            pool = await get_pool(repository_id)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE users SET status = 1 WHERE id = %s",
                        (user_id,),
                    )
            logging.info(f"✅ Status set to online for '{username}' (id={user_id})")
        except Exception as e:
            logging.warning(f"⚠️ Could not update online status for '{username}': {e}")

        return user_id

    async def disconnect(self, user_id: int):
        repository_id = self.user_repo.pop(user_id, None)

        # Clean up username reverse map
        self.username_to_id = {u: uid for u, uid in self.username_to_id.items() if uid != user_id}

        if user_id in self.active_sockets:
            del self.active_sockets[user_id]
            logging.info(
                f"User id={user_id} disconnected. Active IDs: {list(self.active_sockets.keys())}"
            )

        if repository_id:
            from utils.database import get_pool
            try:
                pool = await get_pool(repository_id)
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE users SET status = 0 WHERE id = %s",
                            (user_id,),
                        )
                logging.info(f"✅ Status set to offline for id={user_id}")
            except Exception as e:
                logging.warning(f"⚠️ Could not update offline status for id={user_id}: {e}")

    async def resolve_username_to_id(self, username: str, repository_id: str) -> Optional[int]:
        """
        Resolve a username to user_id — uses in-memory cache first,
        falls back to DB lookup (for offline users not in active_sockets).
        """
        if username in self.username_to_id:
            return self.username_to_id[username]
        return await self.resolve_user_id(username, repository_id)

    async def send_to_user_id(self, message: str, user_id: int) -> bool:
        if user_id not in self.active_sockets:
            logging.info(f"User id={user_id} NOT in active sockets.")
            return False

        socket = self.active_sockets[user_id]
        try:
            await socket.send_text(message)
            return True
        except (ConnectionClosed, RuntimeError) as e:
            logging.info(f"Stale connection for id={user_id}, cleaning up. Error: {e}")
            await self.disconnect(user_id)
            return False

    async def broadcast(self, message: str):
        for socket in self.active_sockets.values():
            await socket.send_text(message)

    def get_online_user_ids(self):
        return list(self.active_sockets.keys())


manager = ConnectionManager()