from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import json
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# (template_id, repo) → set of connected websockets
rooms: dict[tuple[int, str], set[WebSocket]] = {}
# websocket → username (for presence)
connections: dict[WebSocket, str] = {}

def frame_preview(data: bytes, limit: int = 24) -> str:
    preview = data[:limit].hex(" ")
    if len(data) > limit:
        return f"{preview} ..."
    return preview

async def broadcast_presence(key: tuple[int, str]):
    users = list(connections[ws] for ws in rooms.get(key, set()) if ws in connections)
    msg = json.dumps({"type": "presence", "users": users})
    logger.debug("[collab] broadcasting presence for %s: %s", key, users)
    for ws in list(rooms.get(key, set())):
        try:
            await ws.send_text(msg)
            logger.debug(
                "[collab] sent presence to '%s' for %s",
                connections.get(ws, "<unknown>"),
                key,
            )
        except Exception:
            logger.warning(
                "[collab] failed to send presence to '%s' for %s",
                connections.get(ws, "<unknown>"),
                key,
                exc_info=True,
            )

@router.websocket("/api/ws/doc/{template_id}")
async def doc_collab_ws(
    websocket: WebSocket,
    template_id: int,
    repo: str = Query(...),
    username: str = Query(...),
):
    await websocket.accept()
    key = (template_id, repo)
    rooms.setdefault(key, set()).add(websocket)
    connections[websocket] = username
    logger.info(
        "[collab] '%s' joined template %s repo '%s' (%s user(s) connected)",
        username,
        template_id,
        repo,
        len(rooms[key]),
    )
    await broadcast_presence(key)

    try:
        while True:
            # Yjs sends binary frames — just relay to all peers
            data = await websocket.receive_bytes()
            peers = [peer for peer in list(rooms[key]) if peer is not websocket]
            logger.debug(
                "[collab] received %s byte(s) from '%s' for template %s repo '%s'; relaying to %s peer(s); preview=%s",
                len(data),
                username,
                template_id,
                repo,
                len(peers),
                frame_preview(data),
            )
            for peer in peers:
                peer_username = connections.get(peer, "<unknown>")
                try:
                    await peer.send_bytes(data)
                    logger.debug(
                        "[collab] sent %s byte(s) from '%s' to '%s' for template %s repo '%s'; preview=%s",
                        len(data),
                        username,
                        peer_username,
                        template_id,
                        repo,
                        frame_preview(data),
                    )
                except Exception:
                    logger.warning(
                        "[collab] failed to relay %s byte(s) from '%s' to '%s' for template %s repo '%s'",
                        len(data),
                        username,
                        peer_username,
                        template_id,
                        repo,
                        exc_info=True,
                    )
    except WebSocketDisconnect:
        logger.info(
            "[collab] '%s' disconnected from template %s repo '%s'",
            username,
            template_id,
            repo,
        )
    finally:
        rooms[key].discard(websocket)
        connections.pop(websocket, None)
        if not rooms[key]:
            del rooms[key]
        await broadcast_presence(key)
        logger.info(
            "[collab] '%s' left template %s repo '%s' (%s user(s) remaining)",
            username,
            template_id,
            repo,
            len(rooms.get(key, set())),
        )
