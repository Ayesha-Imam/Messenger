import aiomysql
from fastapi import APIRouter, HTTPException, Path, Query
from models.group import Group, GroupCreateRequest
from utils.database import get_pool, init_db
from datetime import datetime

router = APIRouter()


async def _resolve_username(username: str, cur) -> int:
    """Resolve a username to its integer user id within an open cursor."""
    await cur.execute(
        "SELECT id FROM users WHERE username = %s AND enabled = '1' LIMIT 1",
        (username,),
    )
    row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found.")
    return row["id"]


# ── Create a group ────────────────────────────────────────────────────────────

@router.post("/create", status_code=201)
async def create_group(
    group_data: GroupCreateRequest,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    """
    GroupCreateRequest.creator_id and members are usernames (strings).
    We resolve them to integer IDs before inserting.
    """
    await init_db(repo)
    pool = await get_pool(repo)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:

            # 1. Unique name check
            await cur.execute(
                "SELECT id FROM messenger_groups WHERE name = %s", (group_data.name,)
            )
            if await cur.fetchone():
                raise HTTPException(status_code=400, detail="Group name already exists.")

            # 2. Resolve creator username → id
            creator_id = await _resolve_username(group_data.creator_id, cur)

            # 3. Resolve all member usernames → ids
            member_ids = []
            for username in group_data.members:
                member_ids.append(await _resolve_username(username, cur))

            # 4. Insert group
            created_at = datetime.now()
            await cur.execute(
                """
                INSERT INTO messenger_groups (name, creator_id, group_type, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (group_data.name, creator_id, group_data.group_type, created_at),
            )
            group_id = cur.lastrowid

            # 5. Insert members
            if member_ids:
                values = []
                for mid in member_ids:
                    values.extend([group_id, mid])
                await cur.execute(
                    "INSERT IGNORE INTO group_members (group_id, member_id) VALUES "
                    + ", ".join(["(%s, %s)"] * len(member_ids)),
                    values,
                )

    return {
        "id": group_id,
        "name": group_data.name,
        "creator_id": creator_id,
        "creator_username": group_data.creator_id,
        "members": member_ids,
        "member_usernames": group_data.members,
        "group_type": group_data.group_type,
        "created_at": created_at.isoformat(),
    }


# ── Get all groups a user belongs to ─────────────────────────────────────────

@router.get("/user/{username}")
async def get_groups_for_user(
    username: str = Path(...),
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:

            # Resolve username → id
            await cur.execute(
                "SELECT id FROM users WHERE username = %s AND enabled = '1' LIMIT 1",
                (username,),
            )
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"User '{username}' not found.")
            user_id = row["id"]

            # Fetch groups
            await cur.execute(
                """
                SELECT g.id, g.name, g.creator_id, g.group_type, g.created_at,
                       u.username AS creator_username
                FROM   messenger_groups g
                JOIN   group_members gm ON gm.group_id = g.id
                JOIN   users u ON u.id = g.creator_id
                WHERE  gm.member_id = %s
                ORDER  BY g.created_at DESC
                """,
                (user_id,),
            )
            groups = await cur.fetchall()

            # Attach member list (ids + usernames) to each group
            for group in groups:
                await cur.execute(
                    """
                    SELECT gm.member_id, u.username
                    FROM   group_members gm
                    JOIN   users u ON u.id = gm.member_id
                    WHERE  gm.group_id = %s
                    """,
                    (group["id"],),
                )
                members = await cur.fetchall()
                group["members"] = [{"id": m["member_id"], "username": m["username"]} for m in members]
                if group.get("created_at"):
                    group["created_at"] = group["created_at"].isoformat()

    return groups