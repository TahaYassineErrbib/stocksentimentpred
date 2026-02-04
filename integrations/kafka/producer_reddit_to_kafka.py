from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from kafka import KafkaProducer


def load_cfg() -> dict:
    cfg_path = Path(__file__).resolve().parents[2] / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    cfg = load_cfg()

    bootstrap = cfg["kafka"]["bootstrap"]
    topic = cfg["kafka"]["topic_reddit"]

    subreddits = cfg["reddit"]["subreddits"]
    keywords = set(k.lower() for k in cfg["reddit"]["keywords"])
    limit = int(cfg["reddit"]["limit_per_subreddit"])

    user_agent = "bigdata-tsla-public-json/1.0"
    poll_delay = 30
    per_sub_delay = 1.0

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

    seen = set()  # demo-only, in-memory dedupe

    while True:
        fetched_at = datetime.now(timezone.utc).isoformat()

        for sub in subreddits:
            url = f"https://www.reddit.com/r/{sub}/new.json?limit={limit}"

            try:
                r = requests.get(url, headers={"User-Agent": user_agent}, timeout=30)
                r.raise_for_status()
                children = r.json().get("data", {}).get("children", [])
            except Exception as e:
                print(f"[producer] subreddit={sub} error: {e}")
                time.sleep(2)
                continue

            sent = 0
            for child in children:
                post = child.get("data", {}) or {}
                pid = post.get("id")
                if not pid or pid in seen:
                    continue

                text = f"{post.get('title','')} {post.get('selftext','')}".lower()
                if keywords and not any(k in text for k in keywords):
                    continue

                seen.add(pid)

                event = {
                    "created_iso": fetched_at,
                    "subreddit": sub,
                    "source": "reddit_public_json",
                    "post": post,
                    # keep a convenience text field for quick debugging/search
                    "text": f"{post.get('title','')}\n{post.get('selftext','')}".strip(),
                }

                producer.send(topic, key=pid, value=event)
                sent += 1

            producer.flush()
            print(f"[producer] subreddit={sub} sent={sent}")
            time.sleep(per_sub_delay)

        time.sleep(poll_delay)


if __name__ == "__main__":
    main()
