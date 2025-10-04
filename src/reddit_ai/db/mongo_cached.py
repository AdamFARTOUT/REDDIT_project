# src/reddit_ai/db/mongo_cached.py
from functools import lru_cache
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "reddit_ai")

@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    print("[cached] creating MongoClient once")
    return MongoClient(MONGO_URI)

def get_db():
    return get_client()[MONGO_DB]
