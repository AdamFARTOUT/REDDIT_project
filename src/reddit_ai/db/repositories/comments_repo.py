import logging
import time
from typing import Iterable, Dict, Any, List
from pymongo import UpdateOne
from pymongo.collection import Collection
from ...utils.common import ts_now

logger = logging.getLogger(__name__)

def _build_update_comment(doc: Dict[str, Any]) -> UpdateOne:
    """
    Expects a doc shaped like the comments collector yields:
      _id (= comment_id), comment_id, post_id, subreddit, author, body,
      score, created_utc, parent_id, permalink, is_top_level, sort, ingested_at (optional)
    """
    _id = doc.get("_id") or doc.get("comment_id")
    if not _id:
        raise ValueError("comment doc missing _id/comment_id")

    now = ts_now()

    update = {
        "$setOnInsert": {
            "comment_id": doc.get("comment_id", _id),
            "post_id": doc.get("post_id"),
            "subreddit": doc.get("subreddit"),
            "created_utc": doc.get("created_utc"),
            "parent_id": doc.get("parent_id"),
            "permalink": doc.get("permalink"),
            "is_top_level": bool(doc.get("is_top_level", False)),
            "first_seen_at": now,
        },
        "$set": {
            "author": doc.get("author"),
            "body": doc.get("body", ""),
            "score": int(doc.get("score", 0)),
            "sort": doc.get("sort"),                 # 'new' | 'top' | 'confidence' | 'old' | ...
            "ingested_at": doc.get("ingested_at", now),
            "last_seen_at": now,
        },
        "$max": {
            "score_max": int(doc.get("score", 0)),
        },
    }
    return UpdateOne({"_id": _id}, update, upsert=True)

def upsert_comments(db, docs_iter: Iterable[Dict[str, Any]], batch_size: int = 500) -> Dict[str, int]:
    coll: Collection = db.comments
    ops: List[UpdateOne] = []
    stats = {"seen": 0, "batches": 0, "upserted": 0, "modified": 0, "matched": 0}

    def flush_ops():
        nonlocal ops, stats
        if not ops:
            return
        try:
            res = coll.bulk_write(ops, ordered=False)
        except Exception:
            logger.warning("comments bulk_write failed; retrying once after 0.5s (ops=%d)", len(ops))
            time.sleep(0.5)
            try:
                res = coll.bulk_write(ops, ordered=False)
            except Exception:
                logger.exception("retry failed; dropping batch (ops=%d)", len(ops))
                ops = []
                return
        stats["batches"] += 1
        stats["matched"] += res.matched_count
        stats["modified"] += res.modified_count
        stats["upserted"] += len(res.upserted_ids or {})
        logger.info("comments bulk_write: matched=%d modified=%d upserted=%d",
                    res.matched_count, res.modified_count, len(res.upserted_ids or {}))
        ops = []

    for doc in docs_iter:
        stats["seen"] += 1
        try:
            ops.append(_build_update_comment(doc))
        except Exception:
            logger.exception("invalid comment doc skipped: %r", doc)
            continue
        if len(ops) >= batch_size:
            flush_ops()

    flush_ops()
    logger.info("comments upsert complete: %s", stats)
    return stats
