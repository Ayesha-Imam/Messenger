import aiomysql
from fastapi import APIRouter, Query
from utils.database import get_pool

router = APIRouter()


# ── Private (1-to-1) messages ─────────────────────────────────────────────────

@router.get("/private/{sender_id}/{receiver_id}")
async def get_private_messages(
    sender_id: str,
    receiver_id: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, sender_id, receiver_id, content,
                       file_name, file_url, timestamp
                FROM   private_messages
                WHERE  (sender_id = %s AND receiver_id = %s)
                   OR  (sender_id = %s AND receiver_id = %s)
                ORDER  BY timestamp ASC
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
    group_id: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, sender_id, group_id, content,
                       file_name, file_url, timestamp
                FROM   group_messages
                WHERE  group_id = %s
                ORDER  BY timestamp ASC
                """,
                (group_id,),
            )
            messages = await cur.fetchall()

    for msg in messages:
        if msg.get("timestamp"):
            msg["timestamp"] = msg["timestamp"].isoformat()
    return messages