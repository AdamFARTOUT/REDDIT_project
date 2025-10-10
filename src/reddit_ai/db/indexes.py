from pymongo import ASCENDING, DESCENDING

def create_indexes(db):
    posts = db.posts
    comments = db.comments

    # --- indexes (idempotent) ---
    posts.create_index([("subreddit", ASCENDING), ("created_utc", DESCENDING)])
    comments.create_index([("post_id", ASCENDING), ("created_utc", DESCENDING)])