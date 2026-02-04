from __future__ import annotations

import json
import time
from typing import Dict, Any, List

from kafka import KafkaProducer

from src.utils.config import load_config
from src.collection.reddit_public_collector import fetch_subreddit_feed, normalize_post


def _json_bytes(d: Dict[str, Any]) -> bytes:
    return json.dumps(d, ensure_ascii=False).encode("utf-8")


def main() -> None:
    cfg = load_config("config.yaml")
    topic = cfg["kafka"]["topic_reddit"]
    bootstrap = cfg["kafka"]["bootstrap"]

    producer = KafkaProducer(bootstrap_servers=bootstrap)

    subreddits: List[str] = cfg["reddit"]["subreddits"]
    keywords: List[str] = cfg["reddit"]["keywords"]
    limit = int(cfg["reddit"]["limit_per_subreddit"])

    seen_ids: set[str] = set()
    feeds = ["new", "hot", "top"]

    print(f"[producer] streaming to Kafka topic: {topic}")

    while True:
        for sr in subreddits:
            for feed in feeds:
                posts = fetch_subreddit_feed(sr, feed, limit=limit, user_agent="bigdata-tsla/1.0")
                for p in posts:
                    norm = normalize_post(p)
                    if not norm.get("_id"):
                        continue
                    text = norm.get("text", "")
                    if not any(k.lower() in text.lower() for k in keywords):
                        continue
                    if norm["_id"] in seen_ids:
                        continue
                    producer.send(topic, _json_bytes(norm))
                    seen_ids.add(norm["_id"])
                producer.flush()
                time.sleep(1.2)
        # avoid unbounded growth
        if len(seen_ids) > 50000:
            seen_ids = set(list(seen_ids)[-20000:])
        time.sleep(5.0)


if __name__ == "__main__":
    main()
