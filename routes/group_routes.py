import aiomysql
from fastapi import APIRouter, HTTPException, Path, Query
from models.group import Group, GroupCreateRequest
from utils.database import get_pool, init_db
from datetime import datetime

router = APIRouter()


# ── Create a group ────────────────────────────────────────────────────────────

@router.post("/create", response_model=Group, status_code=201)
async def create_group(
    group_data: GroupCreateRequest,
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
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

            # 2. Creator must exist
            await cur.execute(
                "SELECT id FROM users WHERE username = %s", (group_data.creator_id,)
            )
            if not await cur.fetchone():
                raise HTTPException(status_code=404, detail="Creator user not found.")

            # 3. All members must exist
            if group_data.members:
                placeholders = ", ".join(["%s"] * len(group_data.members))
                await cur.execute(
                    f"SELECT username FROM users WHERE username IN ({placeholders})",
                    group_data.members,
                )
                found = await cur.fetchall()
                if len(found) != len(group_data.members):
                    raise HTTPException(status_code=400, detail="Some members do not exist.")

            # 4. Insert group
            created_at = datetime.now()
            await cur.execute(
                """
                INSERT INTO messenger_groups (name, creator_id, group_type, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (group_data.name, group_data.creator_id, group_data.group_type, created_at),
            )
            group_id = cur.lastrowid

            # 5. Insert members
            if group_data.members:
                values = []
                for m in group_data.members:
                    values.extend([group_id, m])
                await cur.execute(
                    "INSERT IGNORE INTO group_members (group_id, member_id) VALUES "
                    + ", ".join(["(%s, %s)"] * len(group_data.members)),
                    values,
                )

    return Group(
        id=group_id,
        name=group_data.name,
        creator_id=group_data.creator_id,
        members=group_data.members,
        group_type=group_data.group_type,
        created_at=created_at,
    )


# ── Get all groups a user belongs to ─────────────────────────────────────────

@router.get("/user/{user_id}")
async def get_groups_for_user(
    user_id: str = Path(...),
    repo: str = Query(..., description="Repository ID, e.g. eaxee_00002e"),
):
    pool = await get_pool(repo)

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:

            await cur.execute(
                """
                SELECT g.id, g.name, g.creator_id, g.group_type, g.created_at
                FROM   messenger_groups g
                JOIN   group_members gm ON gm.group_id = g.id
                WHERE  gm.member_id = %s
                ORDER  BY g.created_at DESC
                """,
                (user_id,),
            )
            groups = await cur.fetchall()

            for group in groups:
                await cur.execute(
                    "SELECT member_id FROM group_members WHERE group_id = %s",
                    (group["id"],),
                )
                rows = await cur.fetchall()
                group["members"] = [r["member_id"] for r in rows]
                if group.get("created_at"):
                    group["created_at"] = group["created_at"].isoformat()

    return groups