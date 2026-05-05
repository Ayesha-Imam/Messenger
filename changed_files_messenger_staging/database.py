# # RSG

# from motor.motor_asyncio import AsyncIOMotorClient
# from dotenv import load_dotenv
# import os

# load_dotenv()
# MONGO_HOST = os.getenv("MONGO_HOST")
# MONGO_PORT = os.getenv("MONGO_PORT")
# MONGO_USER = os.getenv("MONGO_USER")
# MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
# MONGO_DB = os.getenv("MONGO_DB")
# TLS_CA_FILE = os.getenv("TLS_CA_FILE")

# # MONGO_URL = "mongodb+srv://hammadgohar80:test%21%40%23@cluster0.wumm88o.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# MONGO_URI = (
#     f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
#     f"{MONGO_DB}?tls=true&tlsCAFile={TLS_CA_FILE}"
# )

# client = AsyncIOMotorClient(MONGO_URI)

# db = client["messenger"]

# users_collection = db["users"]
# groups_collection = db["groups"]

# # Separate collections for messages
# private_messages_collection = db["private_messages"]
# group_messages_collection = db["group_messages"]




# local + staging

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URL = os.getenv("MONGO_URI")

# MONGO_URL = "mongodb+srv://hammadgohar80:test%21%40%23@cluster0.wumm88o.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = AsyncIOMotorClient(MONGO_URL)

db = client["messenger"]

users_collection = db["users"]
groups_collection = db["groups"]

# Separate collections for messages
private_messages_collection = db["private_messages"]
group_messages_collection = db["group_messages"]
