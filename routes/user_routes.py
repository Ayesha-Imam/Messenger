import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from utils.database import users_collection

router = APIRouter()

# Request payload model
class RepositoryRequest(BaseModel):
    repository_id: str

@router.post("/")  # Changed to POST to accept payload
async def get_users(request: RepositoryRequest):
    # Filter by repository_id
    users = await users_collection.find(
        {"repositoryId": request.repository_id}
    ).to_list(1000)

    if not users:
        raise HTTPException(status_code=404, detail="No users found for this repository_id")

    updated_users = []
    for user in users:
        updated_user = {
            "id": str(user["_id"]),
            "fullName": user["fullName"],
            "online": user.get("is_online", False)
        }
        updated_users.append(updated_user)

    logging.info(
        f"📥 Repo: {request.repository_id} | Total users: {len(updated_users)} "
        f"| Online: {[u['fullName'] for u in updated_users if u['online']]}"
    )
    return updated_users
