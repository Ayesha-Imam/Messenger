import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import json
import logging
import aiomysql
from utils.database import get_pool

router = APIRouter()
logger = logging.getLogger(__name__)

# (template_id, repo) → { "users": set[WebSocket], "seeded": bool }
rooms: dict[tuple[int, str, str], dict] = {}

# websocket → username
connections: dict[WebSocket, str] = {}

HEARTBEAT_INTERVAL = 15  # must be well under the client's 30s watchdog

last_bytes: dict[WebSocket, bytes | None] = {}

async def heartbeat_loop(websocket: WebSocket):
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            data = last_bytes.get(websocket)
            if data is not None:
                try:
                    await websocket.send_bytes(data)
                except Exception:
                    return  # socket already gone, loop will be cancelled in finally
    except asyncio.CancelledError:
        return


def frame_preview(data: bytes, limit: int = 24) -> str:
    preview = data[:limit].hex(" ")
    return f"{preview} ..." if len(data) > limit else preview


async def load_content_from_db(template_id: int, repo: str, mode: str) -> dict | None:
    """Fetch TipTap JSON content from doc_template. Returns parsed dict or None."""
    try:
        pool = await get_pool(repo)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                db = f"{repo}_metamodel"
                logger.info("database ka naaam: '%s'", db)
                if mode == "template":
                    await cur.execute(
                        f"SELECT content FROM `{db}`.doc_template WHERE id = %s",
                        (template_id,)
                    )
                else:
                    await cur.execute(
                        f"SELECT content FROM `{db}`.doc_submission WHERE id = %s",
                        (template_id,)
                    )  
                row = await cur.fetchone()
                if not row or not row["content"]:
                    return None
                content = row["content"]
                # content is stored as JSON string in LONGTEXT
                if isinstance(content, str):
                    return json.loads(content)
                return content
    except Exception:
        logger.error(
            "[collab] failed to load content for template %s repo '%s'",
            template_id, repo, exc_info=True
        )
        return None


async def broadcast_presence(key: tuple[int, str]):
    room = rooms.get(key)
    if not room:
        return
    users = [connections[ws] for ws in room["users"] if ws in connections]
    msg = json.dumps({"type": "presence", "users": users})
    for ws in list(room["users"]):
        try:
            await ws.send_text(msg)
        except Exception:
            logger.warning(
                "[collab] failed to send presence to '%s'",
                connections.get(ws, "<unknown>"), exc_info=True
            )


@router.websocket("/api/ws/doc/{template_id}")
async def doc_collab_ws(
    websocket: WebSocket,
    template_id: int,
    repo: str = Query(...),
    username: str = Query(...),
    mode: str = Query(...),
):
    await websocket.accept()
    key = (template_id, repo, mode)
    
    last_bytes[websocket] = None
    heartbeat_task = asyncio.create_task(heartbeat_loop(websocket))

    # ── Room init ──────────────────────────────────────────────────────────
    is_first_joiner = key not in rooms or len(rooms[key]["users"]) == 0
    if key not in rooms:
        rooms[key] = {"users": set(), "seeded": False}

    rooms[key]["users"].add(websocket)
    connections[websocket] = username

    logger.info(
        "[collab] '%s' joined template %s repo '%s' (%s user(s) connected)",
        username, template_id, repo, len(rooms[key]["users"])
    )
    
    logger.info(
    "%s CONNECT username='%s' websocket=%s",
    rooms[key],
    username,
    id(websocket),
)

    # ── Seed first joiner from DB if room has never been seeded ───────────
    # Only sent to THIS user, only once per room lifetime.
    # Their frontend calls setContent → Yjs picks it up → syncs to all future peers.
    if is_first_joiner and not rooms[key]["seeded"]:
        content = await load_content_from_db(template_id, repo, mode)
        if content:
            seed_msg = json.dumps({"type": "seed", "content": content})
            try:
                await websocket.send_text(seed_msg)
                rooms[key]["seeded"] = True
                logger.info(
                    "[collab] sent seed content to first joiner '%s' for template %s",
                    username, template_id
                )
            except Exception:
                logger.error(
                    "[collab] failed to send seed to '%s'", username, exc_info=True
                )
        else:
            # No content in DB yet — mark seeded anyway so we don't retry
            rooms[key]["seeded"] = True
            logger.info(
                "[collab] no DB content for template %s, room starts empty", template_id
            )

    await broadcast_presence(key)

    try:
        while True:
            message = await websocket.receive()

            if message["type"] == "websocket.receive":
                if message.get("bytes"):
                    # ── Binary: Yjs update, relay to all peers ─────────────
                    data = message["bytes"]
                    last_bytes[websocket] = data
                    peers = [p for p in rooms[key]["users"] if p is not websocket]
                    logger.debug(
                        "[collab] BINARY %s bytes from '%s', relaying to %s peer(s); preview=%s",
                        len(data), username, len(peers), frame_preview(data)
                    )
                    for peer in peers:
                        try:
                            await peer.send_bytes(data)
                        except Exception:
                            logger.warning(
                                "[collab] failed to relay to '%s'",
                                connections.get(peer, "<unknown>"), exc_info=True
                            )

                elif message.get("text"):
                    # ── Text: log and ignore, don't relay ─────────────────
                    # Yjs awareness/custom messages from frontend arrive here.
                    # We don't relay text — presence is server-managed.
                    logger.debug(
                        "[collab] TEXT from '%s': %s",
                        username, message["text"][:200]
                    )

    except WebSocketDisconnect:
        logger.info("[collab] '%s' disconnected (clean) from template %s", username, template_id)
    except RuntimeError as e:
        if "disconnect message has been received" in str(e):
            logger.info("[collab] '%s' disconnected (abrupt) from template %s", username, template_id)
        else:
            logger.error("[collab] unexpected RuntimeError for '%s': %s", username, e, exc_info=True)
            raise
    finally:
        heartbeat_task.cancel()
        last_bytes.pop(websocket, None)
        rooms[key]["users"].discard(websocket)
        connections.pop(websocket, None)

        if not rooms[key]["users"]:
            # Last user left — evict room so next open re-seeds from DB
            # This ensures any saved changes are picked up on next open
            del rooms[key]
            logger.info(
                "[collab] room evicted for template %s repo '%s'", template_id, repo
            )

        await broadcast_presence(key)
        logger.info(
            "[collab] '%s' left template %s (%s remaining)",
            username, template_id, len(rooms.get(key, {}).get("users", set()))
        )