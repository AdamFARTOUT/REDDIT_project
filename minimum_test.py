import os, time
from datetime import datetime, timezone
from dotenv import load_dotenv
import praw
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
posts = db.posts
comments = db.comments

# --- indexes (idempotent) ---
posts.create_index([("post_id", ASCENDING)], unique=True)
posts.create_index([("subreddit", ASCENDING), ("created_utc", DESCENDING)])
comments.create_index([("comment_id", ASCENDING)], unique=True)
comments.create_index([("post_id", ASCENDING), ("created_utc", DESCENDING)])

SUBREDDITS = ["artificial", "MachineLearning", "LocalLLaMA", "ChatGPT"]
POSTS_PER_LISTING = 150   # tune
COMMENTS_PER_POST = 20    # top-level cap

def ts_now():
    return datetime.now(timezone.utc)

def clean_text(s):
    return (s or "").strip()

def upsert_posts(sub_name, listing="new", limit=100):
    sr = reddit.subreddit(sub_name)
    if listing == "new":
        gen = sr.new(limit=limit)
    elif listing == "hot":
        gen = sr.hot(limit=limit)
    else:
        gen = sr.top(time_filter="day", limit=limit)

    ops = []
    count = 0
    for p in gen:
        doc = {
            "post_id": p.id,
            "subreddit": str(p.subreddit),
            "title": clean_text(getattr(p, "title", "")),
            "selftext": clean_text(getattr(p, "selftext", "")),
            "author": str(p.author) if p.author else None,
            "score": int(getattr(p, "score", 0)),
            "upvote_ratio": float(getattr(p, "upvote_ratio", 0) or 0),
            "num_comments": int(getattr(p, "num_comments", 0)),
            "permalink": getattr(p, "permalink", ""),
            "created_utc": int(getattr(p, "created_utc", 0)),
            "ingested_at": ts_now(),
            "listing": listing,
        }
        ops.append(UpdateOne({"post_id": p.id}, {"$set": doc}, upsert=True))
        count += 1
        if count % 100 == 0:
            posts.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        posts.bulk_write(ops, ordered=False)

def fetch_top_comments_for_recent_posts(limit_posts=200, comments_per_post=20):
    # last ingested posts by time
    for p in posts.find({}, {"post_id":1, "subreddit":1}).sort("created_utc", DESCENDING).limit(limit_posts):
        submission = reddit.submission(id=p["post_id"])
        submission.comments.replace_more(limit=0)
        top_level = submission.comments[:comments_per_post]
        ops = []
        for c in top_level:
            doc = {
                "comment_id": c.id,
                "post_id": p["post_id"],
                "subreddit": p["subreddit"],
                "author": str(c.author) if c.author else None,
                "body": clean_text(getattr(c, "body", "")),
                "score": int(getattr(c, "score", 0)),
                "created_utc": int(getattr(c, "created_utc", 0)),
                "parent_id": getattr(c, "parent_id", None),
                "ingested_at": ts_now(),
            }
            if doc["body"] and doc["body"].lower() not in ("[deleted]", "[removed]"):
                ops.append(UpdateOne({"comment_id": c.id}, {"$set": doc}, upsert=True))
        if ops:
            comments.bulk_write(ops, ordered=False)
        time.sleep(0.5)  # be gentle

if __name__ == "__main__":
    for sr in SUBREDDITS:
        upsert_posts(sr, "new", POSTS_PER_LISTING)
        time.sleep(1)
        print("Fetched posts for /r/", sr)
        upsert_posts(sr, "hot", POSTS_PER_LISTING//2)
        time.sleep(1)
        print("Fetched posts for /r/", sr)
        upsert_posts(sr, "top", POSTS_PER_LISTING//2)
        time.sleep(1)
        print("Fetched posts for /r/", sr)

    fetch_top_comments_for_recent_posts(limit_posts=200, comments_per_post=COMMENTS_PER_POST)
    print("Ingestion done.")
# minimum_test