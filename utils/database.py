from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    # Fallback: Build URI from components if MONGO_URI is not set
    MONGO_HOST = os.getenv("MONGO_HOST")
    MONGO_PORT = os.getenv("MONGO_PORT")
    MONGO_USER = os.getenv("MONGO_USER")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
    MONGO_DB = os.getenv("MONGO_DB")
    TLS_CA_FILE = os.getenv("TLS_CA_FILE")

    mongo_uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        f"{MONGO_DB}?tls=true&tlsCAFile={TLS_CA_FILE}"
    )

client = AsyncIOMotorClient(mongo_uri)

db = client["messenger"]

users_collection = db["users"]
groups_collection = db["groups"]

# Separate collections for messages
private_messages_collection = db["private_messages"]
group_messages_collection = db["group_messages"]
