import time
import logging
from pymongo import UpdateOne
import praw
from ..db.mongo import get_db 
from ..config import REDDIT


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

reddit= praw.Reddit(**REDDIT)