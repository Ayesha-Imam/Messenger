"""
Real-time deployment events for everyone logged in to a repository.

The Java app calls POST /api/internal/deployment-events whenever a deployment is scheduled,
cancelled, started, finished or failed. Every browser logged in to that repository keeps a
WebSocket open on /api/ws/system/events and receives the event as JSON:

    {"type": "deployment_scheduled", "schedule": {...}}

This channel is separate from the messenger socket (/api/ws/{username}) and doesn't change it.
Its path has two parts after /api/ws/ on purpose: the messenger route is declared directly on the
app before routers are included, and "/api/ws/system" would match it as username "system".
It only sends; anything a browser sends except "ping" is ignored.

Configuration (environment variable of the FastAPI process):
    INTERNAL_EVENTS_TOKEN   shared secret; Java sends the same value in X-Internal-Token.
                            Without it the publish endpoint refuses every request.

Connections are kept in this process's memory. With several uvicorn workers, each worker only
reaches the browsers connected to it; run one worker (as now) or add Redis pub/sub later.
"""

import asyncio
import hmac
import json
import logging
import os
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

router = APIRouter()

KNOWN_EVENT_TYPES = {
    "deployment_scheduled",
    "deployment_cancelled",
    "deployment_started",
    "deployment_completed",
    "deployment_failed",
    "deployment_missed",
    "backup_restored",
}


class SystemEventHub:
    """Open system-event sockets, grouped by repository (compared case-insensitively)."""

    def __init__(self) -> None:
        self._clients: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(repo: str) -> str:
        return repo.strip().lower()

    async def add(self, repo: str, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.setdefault(self._key(repo), set()).add(websocket)

    async def remove(self, repo: str, websocket: WebSocket) -> None:
        async with self._lock:
            clients = self._clients.get(self._key(repo))
            if clients is not None:
                clients.discard(websocket)
                if not clients:
                    del self._clients[self._key(repo)]

    def count(self, repo: str) -> int:
        return len(self._clients.get(self._key(repo), ()))

    async def broadcast(self, repo: str, payload: Dict[str, Any]) -> int:
        """Sends payload to every socket of the repository. Returns how many received it."""
        async with self._lock:
            targets = list(self._clients.get(self._key(repo), ()))
        message = json.dumps(payload)

        async def send(websocket: WebSocket) -> bool:
            try:
                await asyncio.wait_for(websocket.send_text(message), timeout=5)
                return True
            except Exception:
                await self.remove(repo, websocket)
                # Close it too, so the browser notices and reconnects (it re-reads the current
                # state then) instead of staying on a socket that no longer gets events.
                try:
                    await websocket.close(code=1011)
                except Exception:
                    pass
                return False

        results = await asyncio.gather(*(send(ws) for ws in targets))
        return sum(1 for ok in results if ok)


hub = SystemEventHub()


@router.websocket("/api/ws/system/events")
async def system_events_socket(
    websocket: WebSocket,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
    username: str = Query("", description="For logging only"),
):
    await websocket.accept()
    await hub.add(repo, websocket)
    logging.info(f"🛰️ System events: '{username}' connected to repo '{repo}' ({hub.count(repo)} open)")
    try:
        while True:
            text = await websocket.receive_text()
            if text == "ping":  # keep-alive from the browser
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception as e:  # a broken connection must not leave the socket registered
        logging.info(f"System events socket for '{username}' closed: {e}")
    finally:
        await hub.remove(repo, websocket)


class DeploymentEventIn(BaseModel):
    repo: str
    event: Dict[str, Any]


@router.post("/api/internal/deployment-events")
async def publish_deployment_event(
    body: DeploymentEventIn,
    x_internal_token: Optional[str] = Header(default=None),
):
    expected = os.getenv("INTERNAL_EVENTS_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="INTERNAL_EVENTS_TOKEN is not configured")
    if not x_internal_token or not hmac.compare_digest(x_internal_token, expected):
        raise HTTPException(status_code=401, detail="Invalid internal token")

    event_type = body.event.get("type")
    if event_type not in KNOWN_EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown event type: {event_type}")
    if not body.repo.strip():
        raise HTTPException(status_code=400, detail="repo is required")

    delivered = await hub.broadcast(body.repo, body.event)
    logging.info(f"📣 {event_type} for repo '{body.repo}' delivered to {delivered} browser(s)")
    return {"delivered": delivered}