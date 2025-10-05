import logging
from ..config import MONGO_URI, MONGO_DB
from pymongo import MongoClient
from functools import lru_cache
from .indexes import create_indexes
logger = logging.getLogger(__name__)
@lru_cache()
def get_client():
    logger.debug("Creating new MongoDB client. (singleton)")
    client = MongoClient(MONGO_URI,serverSelectionTimeoutMS=3000)
    try:
        client.admin.command('ping')
        logger.debug("MongoDB connection successful.")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise
    return client
def get_db():
    client = get_client()
    db = client[MONGO_DB]
    return db   
def ensure_indexes():
    db = get_db()
    create_indexes(db)