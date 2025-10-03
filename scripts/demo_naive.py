# scripts/demo_naive_fair.py
import time
from src.reddit_ai.db.mongo_naive import get_db, get_client

def run():
    for i in range(3):
        before = time.perf_counter()
        db = get_db()  # NEW client every time (no cache)
        db.health.insert_one({"i": i, "mode": "naive_fair"})
        took = (time.perf_counter() - before) * 1000
        print(f"[naive] i={i} client_id={id(get_client())} took ~{took:.2f} ms")

if __name__ == "__main__":
    run()
