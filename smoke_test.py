# smoke_test
import os
from dotenv import load_dotenv
import praw

load_dotenv()
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
    username=os.getenv("REDDIT_USERNAME"),   # optional
    password=os.getenv("REDDIT_PASSWORD"),   # optional
)

print("read_only:", reddit.read_only)
for post in reddit.subreddit("python").hot(limit=3):
    print(post.title)