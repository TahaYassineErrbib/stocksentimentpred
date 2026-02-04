from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Iterable, Optional

import requests

from src.utils.mongo import get_collection, bulk_upsert_by_key


def _utc_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _contains_keywords(text: str, keywords: List[str]) -> bool:
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords)

def fetch_subreddit_feed(
    subreddit: str,
    feed: str,
    limit: int = 100,
    user_agent: str = "bigdata-tsla/1.0",
    ) -> list[dict]:
    """
    Fetch posts from a subreddit feed using Reddit public JSON.
    feed: 'new', 'hot', or 'top'
    """
    url = f"https://www.reddit.com/r/{subreddit}/{feed}.json"
    params = {"limit": str(min(limit, 100))}
    if feed == "top":
        params["t"] = "week"

    headers = {"User-Agent": user_agent}
    r = requests.get(url, params=params, headers=headers, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"{subreddit}/{feed} failed {r.status_code}")

    payload = r.json()
    return [c["data"] for c in payload.get("data", {}).get("children", [])]




def normalize_post(d: Dict[str, Any]) -> Dict[str, Any]:
    created = d.get("created_utc")
    created_iso = _utc_iso(int(created)) if created is not None else None

    post_id = d.get("id")
    subreddit = d.get("subreddit")
    title = d.get("title") or ""
    selftext = d.get("selftext") or ""
    score = int(d.get("score") or 0)
    author = d.get("author") or None
    num_comments = int(d.get("num_comments") or 0)
    permalink = d.get("permalink") or ""
    url = "https://www.reddit.com" + permalink if permalink.startswith("/") else d.get("url")

    return {
        "_id": f"t3_{post_id}" if post_id else None,
        "type": "post",
        "source": "reddit_public_json",
        "subreddit": subreddit,
        "created_utc": int(created) if created is not None else None,
        "created_iso": created_iso,
        "title": title,
        "text": (title + "\n" + selftext).strip(),
        "selftext": selftext,
        "score": score,
        "author": author,
        "num_comments": num_comments,
        "permalink": permalink,
        "url": url,
    }


def write_jsonl(path: str | Path, docs: Iterable[Dict[str, Any]]) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for d in docs:
            if not d.get("_id"):
                continue
            # minimal JSONL writer (no external deps)
            import json
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
            n += 1
    return n


def collect_reddit_public(
    subreddits: List[str],
    keywords: List[str],
    limit_per_subreddit: int,
    out_jsonl: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    user_agent: str = "bigdata-tsla/1.0",
    sleep_seconds: float = 1.2,
) -> Dict[str, Any]:
    all_docs: List[Dict[str, Any]] = []
    fetched = 0
    matched = 0

    feeds = ["new", "hot", "top"]

    for sr in subreddits:
        for feed in feeds:
            posts = fetch_subreddit_feed(
                sr,
                feed,
                limit=limit_per_subreddit,
                user_agent=user_agent,
            )
            fetched += len(posts)

            for p in posts:
                norm = normalize_post(p)
                text = norm.get("text", "")
                if _contains_keywords(text, keywords):
                    all_docs.append(norm)
                    matched += 1

            time.sleep(sleep_seconds)


        for p in posts:
            norm = normalize_post(p)
            text = norm.get("text", "")
            if _contains_keywords(text, keywords):
                all_docs.append(norm)
                matched += 1

        time.sleep(sleep_seconds)

    # write snapshot
    written = write_jsonl(out_jsonl, all_docs)

    # upsert into mongo
    col = get_collection(mongo_uri, mongo_db, mongo_collection)
    upserted = bulk_upsert_by_key(col, all_docs, key_field="_id")

    return {
        "subreddits": subreddits,
        "keywords": keywords,
        "fetched_posts": fetched,
        "matched_posts": matched,
        "written_jsonl": written,
        "mongo_upserts": int(upserted),
        "jsonl_path": out_jsonl,
    }
