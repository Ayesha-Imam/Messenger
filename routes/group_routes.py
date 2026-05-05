from fastapi import APIRouter, HTTPException, Path
from models.group import Group, GroupCreateRequest
from utils.database import groups_collection,users_collection, group_messages_collection
from datetime import datetime

router = APIRouter()

# create groups

@router.post("/create", response_model=Group, status_code=201)
async def create_group(group_data: GroupCreateRequest):
    # Check if a group with the same name already exists
    existing = await groups_collection.find_one({"name": group_data.name})
    if existing:
        raise HTTPException(status_code=400, detail="Group name already exists.")
    
    # Check if creator exists
    creator = await users_collection.find_one({"fullName": group_data.creator_id})
    if not creator:
        raise HTTPException(status_code=404, detail="Creator user not found")

    # Optional: ensure all member IDs exist
    cursor = users_collection.find({"fullName": {"$in": group_data.members}})
    found_members = await cursor.to_list(length=None)
    if len(found_members) != len(group_data.members):
        raise HTTPException(status_code=400, detail="Some members do not exist")

    # Build group document
    group = Group(**group_data.model_dump())
    insert_data = group.model_dump(by_alias=True, exclude_none=True)
    result = await groups_collection.insert_one(insert_data)

    # Return inserted group
    created_group = await groups_collection.find_one({"_id": result.inserted_id})
    if not created_group:
        raise HTTPException(status_code=500, detail="Group could not be created.")
    
    # Optional: convert _id to string for response
    created_group["_id"] = str(created_group["_id"])
    return created_group

# get groups of a user

@router.get("/user/{user_id}")
async def get_groups_for_user(user_id: str = Path(...)):
    cursor = groups_collection.find({ "members": user_id })
    groups = await cursor.to_list(length=None)

    # Convert ObjectId to string
    for group in groups:
        group["_id"] = str(group["_id"])

    return groups



# @router.get("/group/{group_id}")
# async def get_group_messages(group_id: str = Path(...)):
#     query = {
#         "group_id": group_id
#     }
#     cursor = group_messages_collection.find(query).sort("timestamp", 1)
#     messages = await cursor.to_list(length=None)

#     for msg in messages:
#         msg["_id"] = str(msg["_id"])
    
#     return messages