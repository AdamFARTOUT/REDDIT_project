import logging
from pymongo import UpdateOne 
from pymongo.collection import Collection
from typing import Iterable, Dict, Any, List
from ..mongo import get_db , ensure_indexes
from ....reddit_ai.utils.common import ts_now
logger = logging.getLogger(__name__)

def _build_update(doc : dict[str, any]) -> UpdateOne :
    _id = doc.get("_id")
    listing = doc.get("listing")
    elem= {
        "$setOnInsert": { "_id": _id, 
                          "post_id": doc.get("post_id"),
                          "subreddit": doc.get("subreddit"), 
                          "created_utc": doc.get("created_utc"),
                          "permalink": doc.get("permalink"),
                          "seent_utc": ts_now(),
                          },
        "$set": { "title": doc.get("title"),
                  "selftext": doc.get("selftext"),
                    "author": doc.get("author"),
                    "score": int(doc.get("score", 0)),
                    "upvote_ratio": doc.get("upvote_ratio", 0),
                    "num_comments": int(doc.get("num_comments", 0)),
                    "ingested_at": doc.get("ingested_at"),
                    "last_seen_at": ts_now(),
                    "listing": listing,
                    "time_filter": doc.get("time_filter")
                },
        "$max": { "max_score": doc.get("score", 0),
                  "max_num_comments": doc.get("num_comments", 0)
                },
        }
    if listing:
        elem["$addToSet"] = {"seen_in": listing}
    return UpdateOne({"_id": _id}, elem, upsert=True)

def upsert_posts(db, docs_iter: Iterable[Dict[str, Any]], batch_size: int = 500) -> Dict[str, int]:
    coll: Collection = get_db()[db]
    ops: List[UpdateOne] = []
    stats = {"seen": 0, "batches": 0, "upserted": 0, "modified": 0, "matched": 0}
    def flush_ops():
        nonlocal ops , stats
        if not ops:
            return
        try:
            res = coll.bulk_write(ops, ordered=False)
            stats["batches"] += 1
            stats["matched"] += res.matched_count
            stats["modified"] += res.modified_count
            stats["upserted"] += len(res.upserted_ids or {})
            logger.info("posts bulk_write: matched=%d modified=%d upserted=%d",
                        res.matched_count, res.modified_count, len(res.upserted_ids or {}))
        finally:
            ops = []
    for doc in docs_iter:
        stats["seen"] += 1
        try:
            ops.append(_build_update(doc))
        except Exception:
            logger.exception("invalid post doc skipped: %r", doc)
            continue
        if len(ops) >= batch_size:
            flush_ops()
    flush_ops()
    logger.info("posts upsert complete: %s", stats)
    return stats