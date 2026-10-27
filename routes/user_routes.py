import logging
import re
import aiomysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from utils.database import get_pool, init_db
import traceback

router = APIRouter()

MESSENGER_ROLES = ("ROLE_ADMIN", "ROLE_USER", "CONTRIBUTOR", "TEMPLATE_EDITOR", "REVIEWER")

USABLE_LICENSE = (
    "l.isLocked = 0 "
    "AND l.expiration_date IS NOT NULL "
    "AND l.expiration_date >= CURDATE()"
)

# pool_name is concatenated into table names
_SAFE_POOL_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,79}$")


class RepositoryRequest(BaseModel):
    repository_id: str


async def _pools_with_roles(cur, roles) -> list[str]:
    """
    Names of pools whose role_type is one of `roles` and whose
    pool_<name>_users and pool_<name>_license tables exist.
    """
    placeholders = ", ".join(["%s"] * len(roles))
    try:
        await cur.execute(
            f"""
            SELECT p.pool_name
            FROM pools p
            WHERE p.role_type IN ({placeholders})
            AND EXISTS (SELECT 1 FROM information_schema.tables t
                        WHERE t.table_schema = DATABASE()
                        AND t.table_name = CONCAT('pool_', p.pool_name, '_users'))
            AND EXISTS (SELECT 1 FROM information_schema.tables t
                        WHERE t.table_schema = DATABASE()
                        AND t.table_name = CONCAT('pool_', p.pool_name, '_license'))
            """,
            roles,
        )
    except aiomysql.Error:
        return []

    return [
        row["pool_name"]
        for row in await cur.fetchall()
        if _SAFE_POOL_NAME.match(row["pool_name"] or "")
    ]


@router.post("/")
async def get_users(request: RepositoryRequest):
    """
    Return all enabled users for the given repository.

    Uses the existing `users` table in {repository_id}_eeatool:
      id, username, user_short_name, email, company_name, metamodel_name,
      user_type, password, enabled, username, cell_no, phone_no,
      extension, image, theme, status, activePoolDetails, ldap_user, user_language

    - company_name  → used to scope users to this repository
    - username → the identifier used throughout the messenger
    - status        → '1' = online, '0' = offline (managed by WebSocket connect/disconnect)
    - enabled       → '1' = active account

    Licensed users are named-licence users with one of MESSENGER_ROLES, plus
    concurrent users: members of any pool with one of those roles that still has
    a usable licence. Concurrent users have no licence of their own until they
    claim one from the pool at login, so named_licence_users alone misses them.
    """
    try:
        # Ensure tables exist for this repo (no-op if already initialised)
        await init_db(request.repository_id)

        pool = await get_pool(request.repository_id)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                placeholders = ", ".join(["%s"] * len(MESSENGER_ROLES))
                licensed_user_ids = f"""
                    SELECT nlu.user_id
                    FROM named_licence_users nlu
                    JOIN license l
                        ON nlu.license_id = l.id
                    WHERE l.user_role IN ({placeholders})
                    AND {USABLE_LICENSE}
                """
                for pool_name in await _pools_with_roles(cur, MESSENGER_ROLES):
                    licensed_user_ids += f"""
                    UNION
                    SELECT pu.user_id
                    FROM `pool_{pool_name}_users` pu
                    WHERE EXISTS (
                        SELECT 1 FROM `pool_{pool_name}_license` pl
                        JOIN license l ON pl.license_id = l.id
                        WHERE {USABLE_LICENSE}
                    )
                    """

                await cur.execute(
                    f"""
                    SELECT
                        u.id,
                        u.username,
                        u.status,
                        u.image
                    FROM users u
                    WHERE u.enabled = '1'
                    AND u.id IN ({licensed_user_ids})
                    ORDER BY u.username ASC
                    """,
                    MESSENGER_ROLES,
                )
                users = await cur.fetchall()

        if not users:
            raise HTTPException(
                status_code=404,
                detail="No users found for this repository_id.",
            )

        result = [
            {
                "id": str(u["id"]),
                "fullName": u["username"],
                "online": u["status"] == 1,
                "image": u["image"],
            }
            for u in users
        ]

        logging.info(
            f"📥 Repo: {request.repository_id} | Total: {len(result)} "
            f"| Online: {[u['fullName'] for u in result if u['online']]}"
        )
        return result
    
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))