from ..config import MONGO_URI, MONGO_DB
from pymongo import MongoClient
from functools import lru_cache
from .indexes import create_indexes

@lru_cache()
def get_client():
    client = MongoClient(MONGO_URI)
    return client
def get_db():
    client = get_client()
    db = client[MONGO_DB]
    return db   
def ensure_indexes():
    db = get_db()
    create_indexes(db)