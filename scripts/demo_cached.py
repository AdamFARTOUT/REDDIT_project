# scripts/demo_cached_fair.py
import time
from src.reddit_ai.db.mongo_cached import get_db, get_client

def run():
    for i in range(3):
        before = time.perf_counter()
        db = get_db()  # same client every time (cached)
        db.health.insert_one({"i": i, "mode": "cached_fair"})
        took = (time.perf_counter() - before) * 1000
        print(f"[cached] i={i} client_id={id(get_client())} took ~{took:.2f} ms")

    print("[cached] cache info:", get_client.cache_info())

if __name__ == "__main__":
    run()
