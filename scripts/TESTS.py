# import os
# from dotenv import load_dotenv

# load_dotenv()
# REDDIT = {
#     "client_id": os.getenv("REDDIT_CLIENT_ID"),
#     "client_secret": os.getenv("REDDIT_CLIENT_SECRET"),
#     "user_agent": os.getenv("REDDIT_USER_AGENT", "RedditAITrend by u/unknown"),
# }
# ## testing how **reddit works
# def test_reddit(client_id, client_secret, user_agent):
#     print(client_id,'\n', client_secret,'\n', user_agent)
    
# # test_reddit(**REDDIT)
# test_reddit(
#     client_id=os.getenv("REDDIT_CLIENT_ID"),
#     client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
#     user_agent=os.getenv("REDDIT_USER_AGENT", "RedditAITrend by u/unknown"),
# )
# from src.reddit_ai.utils.logging_setup import setup_logging
# setup_logging()  # console INFO+, file DEBUG+ to logs/app.log

# from src.reddit_ai.db.mongo import get_db

# db = get_db()
from src.reddit_ai.utils.logging_setup import setup_logging
setup_logging()  # console INFO+, file DEBUG+ to logs/app.log
from src.reddit_ai.collectors.posts import fetch_posts_details
from src.reddit_ai.collectors.comments import fetch_comments_details
from src.reddit_ai.config import REDDIT
import praw
reddit= praw.Reddit(**REDDIT)
for doc in fetch_posts_details(reddit, "Artificial", listing="new", limit=2):
    print(doc ,'\n') # or send to your repo upsert
    for comm in fetch_comments_details(reddit, doc['_id'], limit=2):
        print(comm ,'\n') # or send to your repo upsert
