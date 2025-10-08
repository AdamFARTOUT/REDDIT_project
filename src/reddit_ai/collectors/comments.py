import logging
from typing import Iterable
from ..utils.common import ts_now, is_englishish
from datetime import timedelta

logger = logging.getLogger(__name__)

def fetch_comments_details(
    reddit,
    post_id: Iterable[str],
    sort: str = "new",
    cap : int = 500,
    limit: int = 100,
    top_level_only: bool = True,
    include_nsfw: bool = False,
    skip_bots: bool = True,
    english_only: bool = True,
    debug_samples: int = 3,
):
    """
    Fetch comments for a given Reddit post.

    Args:
        reddit: Authenticated PRAW Reddit instance.
        post_id (str): ID of the Reddit post to fetch comments from.
        limit (int): Maximum number of comments to fetch.
        include_nsfw (bool): Whether to include NSFW comments.
        skip_bots (bool): Whether to skip comments made by bots.
        english_only (bool): Whether to include only English-like comments.
        debug_samples (int): Number of sample comments to log for debugging.
    """
    logger.info(f"Fetching comments for post {post_id} | limit={limit} | include_nsfw={include_nsfw} | skip_bots={skip_bots} | english_only={english_only} | debug_samples={debug_samples} | sort={sort} | top_level_only={top_level_only}")
    comments = []
    try:
        submission = reddit.submission(id=post_id)
        submission.comment_sort = "new"
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            if len(comments) >= limit:
                break
            if skip_bots and comment.author and comment.author.name == "[deleted]":
                continue
            if english_only and not is_englishish(comment.body):
                continue
            comments.append(comment)
            if len(comments) <= debug_samples:
                logger.debug(f"Sample comment: {comment.body}")
    except Exception as e:
        logger.error(f"Error fetching comments for post {post_id}: {e}")
    return comments