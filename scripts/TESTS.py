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
for doc in fetch_posts_details(reddit, "Artificial", listing="top", limit=1,window_days=2000,time_filter="all"):
    print(doc ,'\n') # or send to your repo upsert
    
    
# for comm in fetch_comments_details(reddit, "1o1ggnu", limit= 2):
#     print(comm ,'\n') # or send to your repo upsert
# from pprint import pprint

# c = reddit.comment("nigi4si")
# c.refresh()  # ensure it’s loaded (or you already got it via submission.comments)
# pprint({k: v for k, v in vars(c).items() if not k.startswith("_")})

from pprint import pprint

# sub = reddit.submission(id="1o1ggnu")
# _ = sub.title            # touch one attr to force a fetch
# pprint({k: v for k, v in vars(sub).items() if not k.startswith("_")})



submission = reddit.submission(id="1d6iggo")
submission.comment_sort = "old"
submission.comments.replace_more(limit=0)
all_comments = submission.comments.list()
comment= all_comments[0]
pprint({k: v for k, v in vars(comment).items() if not k.startswith("_")})
