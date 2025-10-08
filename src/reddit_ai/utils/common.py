from datetime import datetime, timezone

def is_englishish(text: str, min_len: int = 10, min_ascii_ratio: float = 0.6) -> bool:
    """Tiny heuristic: require minimal length and enough ASCII letters."""
    if not text:
        return False
    t = text.strip()
    if len(t) < min_len:
        return False
    letters = sum(ch.isascii() and ch.isalpha() for ch in t)
    return (letters / max(1, len(t))) >= min_ascii_ratio
def ts_now():
    return datetime.now(timezone.utc)