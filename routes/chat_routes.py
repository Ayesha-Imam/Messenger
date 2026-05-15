import aiomysql
from fastapi import APIRouter, Query, HTTPException
from utils.database import get_pool

router = APIRouter()


async def _resolve_username(username: str, pool) -> int:
    """Resolve a username to its integer user id. Raises 404 if not found."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id FROM users WHERE username = %s AND enabled = '1' LIMIT 1",
                (username,),
            )
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found.")
    return row[0]


# ── Private (1-to-1) messages ─────────────────────────────────────────────────

@router.get("/private/{sender_username}/{receiver_username}")
async def get_private_messages(
    sender_username: str,
    receiver_username: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)
    sender_id   = await _resolve_username(sender_username, pool)
    receiver_id = await _resolve_username(receiver_username, pool)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT   pm.id, pm.content, pm.file_name, pm.file_url, pm.timestamp,
                         s.username AS sender_username,
                         r.username AS receiver_username,
                         pm.sender_id, pm.receiver_id
                FROM     private_messages pm
                JOIN     users s ON s.id = pm.sender_id
                JOIN     users r ON r.id = pm.receiver_id
                WHERE    (pm.sender_id = %s AND pm.receiver_id = %s)
                   OR    (pm.sender_id = %s AND pm.receiver_id = %s)
                ORDER BY pm.timestamp ASC
                """,
                (sender_id, receiver_id, receiver_id, sender_id),
            )
            messages = await cur.fetchall()

    for msg in messages:
        if msg.get("timestamp"):
            msg["timestamp"] = msg["timestamp"].isoformat()
    return messages


# ── Group messages ────────────────────────────────────────────────────────────

@router.get("/group/{group_id}")
async def get_group_messages(
    group_id: int,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT   gm.id, gm.content, gm.file_name, gm.file_url, gm.timestamp,
                         gm.group_id, gm.sender_id,
                         u.username AS sender_username
                FROM     group_messages gm
                JOIN     users u ON u.id = gm.sender_id
                WHERE    gm.group_id = %s
                ORDER BY gm.timestamp ASC
                """,
                (group_id,),
            )
            messages = await cur.fetchall()

    for msg in messages:
        if msg.get("timestamp"):
            msg["timestamp"] = msg["timestamp"].isoformat()
    return messages