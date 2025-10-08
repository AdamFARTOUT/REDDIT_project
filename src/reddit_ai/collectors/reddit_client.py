import logging
import praw
from ..config import REDDIT

logger = logging.getLogger(__name__)
def get_reddit_client():
    """Initialize and return a Reddit client using PRAW."""
    logger.info("Setting up Reddit client.")
    reddit= praw.Reddit(**REDDIT)
    logger.debug(f"Reddit client ID: {REDDIT.get('client_id')}")
    logger.debug(f"Reddit client secret: {REDDIT.get('client_secret')}")
    logger.debug(f"Reddit user_agent: {REDDIT.get('user_agent')}")
    return reddit
