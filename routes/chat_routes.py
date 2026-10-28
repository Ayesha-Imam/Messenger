import aiomysql
from fastapi import APIRouter, Query, HTTPException
from utils.database import get_pool, init_db

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
    await init_db(repo)
    pool = await get_pool(repo)
    sender_id   = await _resolve_username(sender_username, pool)
    receiver_id = await _resolve_username(receiver_username, pool)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT   pm.id, pm.content, pm.file_name, pm.file_url, pm.timestamp,
                         pm.delivered_at, pm.read_at,
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
        for field in ("timestamp", "delivered_at", "read_at"):
            if msg.get(field):
                msg[field] = msg[field].isoformat()
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


# ── Unread counts ─────────────────────────────────────────────────────────────

@router.get("/unread/{username}")
async def get_unread_counts(
    username: str,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    """
    Unread messages per conversation for `username`:
      {"users": {"<sender username>": n}, "groups": {"<group id>": n}}
    Private: received and not yet read. Group: newer than the member's
    last_read_message_id, excluding their own messages.
    """
    await init_db(repo)
    pool = await get_pool(repo)
    user_id = await _resolve_username(username, pool)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT   s.username, COUNT(*) AS unread
                FROM     private_messages pm
                JOIN     users s ON s.id = pm.sender_id
                WHERE    pm.receiver_id = %s AND pm.read_at IS NULL
                GROUP BY s.username
                """,
                (user_id,),
            )
            users = {row["username"]: row["unread"] for row in await cur.fetchall()}

            await cur.execute(
                """
                SELECT   gm.group_id, COUNT(*) AS unread
                FROM     group_members mem
                JOIN     group_messages gm
                         ON  gm.group_id = mem.group_id
                         AND gm.id > mem.last_read_message_id
                         AND gm.sender_id <> mem.member_id
                WHERE    mem.member_id = %s
                GROUP BY gm.group_id
                """,
                (user_id,),
            )
            groups = {str(row["group_id"]): row["unread"] for row in await cur.fetchall()}

    return {"users": users, "groups": groups}
