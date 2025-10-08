import logging
from typing import Iterator, Dict, Any
from ..utils.common import ts_now, is_englishish

logger = logging.getLogger(__name__)

def fetch_comments_details(
    reddit,
    post_id: str,
    *,
    sort: str = "new",
    cap: int = 500,            # max to scan from API
    limit: int = 100,          # max to yield
    top_level_only: bool = True,
    include_nsfw: bool = False,
    skip_bots: bool = True,
    english_only: bool = True,
    debug_samples: int = 3,
) -> Iterator[Dict[str, Any]]:
    """
    Yield comment docs for a given Reddit post.

    Args:
        reddit: Authenticated PRAW Reddit instance.
        post_id: ID of the Reddit post to fetch comments from.
        sort: 'new' | 'top' | 'best' | 'old'
        cap: Max comments to *scan* from API.
        limit: Max comments to *yield*.
        top_level_only: Only top-level comments if True, else all levels.
        include_nsfw: If False and submission is NSFW, skip entirely.
        skip_bots: Skip authors whose username contains 'bot'.
        english_only: Keep only English-like comments via is_englishish(comment.body).
        debug_samples: Number of sample bodies to log.
    """
    logger.info(
        "Fetching comments for post %s | sort=%s | cap=%d | limit=%d | top_level_only=%s | include_nsfw=%s | skip_bots=%s | english_only=%s | debug_samples=%d",
        post_id, sort, cap, limit, top_level_only, include_nsfw, skip_bots, english_only, debug_samples
    )

    stats = dict(seen=0, yielded=0, skipped_removed=0, skipped_bots=0, skipped_lang=0, skipped_nsfw=0)
    sample_left = debug_samples

    # Fetch submission once
    submission = reddit.submission(id=post_id)
    submission.comment_sort = sort
    submission.comments.replace_more(limit=0)

    # NSFW gate at submission level
    if not include_nsfw and getattr(submission, "over_18", False):
        stats["skipped_nsfw"] += 1
        logger.info("Submission %s is NSFW; skipping. Stats: %s", post_id, stats)
        return

    # Build the iterable of comments
    if top_level_only:
        pool = submission.comments[:cap]
    else:
        all_comments = submission.comments.list()
        pool = all_comments[:cap] if cap is not None else all_comments

    for comment in pool:
        if stats["yielded"] >= limit:
            break

        stats["seen"] += 1

        # removed / deleted
        if getattr(comment, "removed_by_category", None) is not None or comment.author is None:
            stats["skipped_removed"] += 1
            continue
        if str(comment.author).lower() == "[deleted]":
            stats["skipped_removed"] += 1
            continue

        # bot filter
        author_str = str(comment.author)
        if skip_bots and "bot" in author_str.lower():
            stats["skipped_bots"] += 1
            continue

        # language filter
        body = getattr(comment, "body", "") or ""
        if english_only and not is_englishish(body):
            stats["skipped_lang"] += 1
            continue

        doc = {
            "comment_id": comment.id,
            "post_id": post_id,
            "subreddit": submission.subreddit.display_name,
            "author": author_str,
            "body": body,
            "score": int(getattr(comment, "score", 0)),
            "created_utc": int(getattr(comment, "created_utc", 0)),
            "parent_id": getattr(comment, "parent_id", None),
            "ingested_at": ts_now().isoformat(),  # serialize as ISO string
        }

        if sample_left > 0:
            sample_left -= 1
            logger.debug("Sample comment: %s", body[:500])

        yield doc
        stats["yielded"] += 1

    logger.info("Done post %s. Stats: %s", post_id, stats)
