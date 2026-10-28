import logging
from typing import Dict, Optional, Set, Tuple
from fastapi import WebSocket
from websockets.exceptions import ConnectionClosed

# User ids are only unique within a repository's own database, so every
# connection is keyed by (repository_id, user_id).
UserKey = Tuple[str, int]


class ConnectionManager:
    def __init__(self):
        # { (repository_id, user_id): open WebSockets } — a user can have
        # several (one per browser tab); they're online while one is open.
        self.active_sockets: Dict[UserKey, Set[WebSocket]] = {}

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

    async def _set_status(self, repository_id: str, user_id: int, status: int):
        from utils.database import get_pool
        try:
            pool = await get_pool(repository_id)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE users SET status = %s WHERE id = %s",
                        (status, user_id),
                    )
            logging.info(
                f"✅ Status set to {'online' if status else 'offline'} for id={user_id} (repo: {repository_id})"
            )
        except Exception as e:
            logging.warning(f"⚠️ Could not update status for id={user_id} (repo: {repository_id}): {e}")

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
        key = (repository_id, user_id)
        first_socket = key not in self.active_sockets
        self.active_sockets.setdefault(key, set()).add(websocket)

        logging.info(
            f"User '{username}' (id={user_id}) connected (repo: {repository_id}). "
            f"Open sockets for user: {len(self.active_sockets[key])}"
        )

        if first_socket:
            await self._set_status(repository_id, user_id, 1)

        return user_id

    async def disconnect(self, repository_id: str, user_id: int, websocket: WebSocket):
        key = (repository_id, user_id)
        sockets = self.active_sockets.get(key)
        if sockets is None or websocket not in sockets:
            return
        sockets.discard(websocket)
        if sockets:
            logging.info(f"User id={user_id} (repo: {repository_id}) closed a socket; {len(sockets)} still open.")
            return

        del self.active_sockets[key]
        logging.info(f"User id={user_id} (repo: {repository_id}) disconnected.")
        await self._set_status(repository_id, user_id, 0)

    async def resolve_username_to_id(self, username: str, repository_id: str) -> Optional[int]:
        return await self.resolve_user_id(username, repository_id)

    def is_online(self, repository_id: str, user_id: int) -> bool:
        return (repository_id, user_id) in self.active_sockets

    async def send_to_user_id(
        self,
        message: str,
        repository_id: str,
        user_id: int,
        exclude: Optional[WebSocket] = None,
    ) -> bool:
        """Send to every open socket of the user (except `exclude`). True if any received it."""
        key = (repository_id, user_id)
        sockets = [s for s in self.active_sockets.get(key, ()) if s is not exclude]
        if not sockets:
            return False

        delivered = False
        for socket in sockets:
            try:
                await socket.send_text(message)
                delivered = True
            except (ConnectionClosed, RuntimeError) as e:
                logging.info(f"Stale connection for id={user_id} (repo: {repository_id}), cleaning up. Error: {e}")
                await self.disconnect(repository_id, user_id, socket)
        return delivered

    async def broadcast(self, message: str):
        for sockets in list(self.active_sockets.values()):
            for socket in list(sockets):
                await socket.send_text(message)

    def get_online_user_ids(self, repository_id: str):
        return [uid for (repo, uid) in self.active_sockets if repo == repository_id]


manager = ConnectionManager()
