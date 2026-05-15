import logging
import aiomysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from utils.database import get_pool, init_db
import traceback

router = APIRouter()


class RepositoryRequest(BaseModel):
    repository_id: str


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
    """
    try:
        # Ensure tables exist for this repo (no-op if already initialised)
        await init_db(request.repository_id)

        pool = await get_pool(request.repository_id)
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT id, username, status
                    FROM   users
                    WHERE  enabled = '1'
                    ORDER  BY username ASC
                    """,
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