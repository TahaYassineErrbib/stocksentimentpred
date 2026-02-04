from __future__ import annotations

import json
from pathlib import Path

import yaml
from kafka import KafkaConsumer
from pymongo import MongoClient


def load_cfg() -> dict:
    cfg_path = Path(__file__).resolve().parents[2] / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    cfg = load_cfg()

    bootstrap = cfg["kafka"]["bootstrap"]
    topic = cfg["kafka"]["topic_reddit"]

    mongo_uri = cfg["mongo"]["uri"]
    db_name = cfg["mongo"]["db"]
    collection_name = cfg["mongo"]["collections"]["social_raw"]

    client = MongoClient(mongo_uri)
    col = client[db_name][collection_name]

    # dedupe on Reddit post id inside the stored payload
    col.create_index("post.id", unique=True, sparse=True)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        group_id="reddit_to_mongo",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    print(f"[consumer] topic={topic} → mongo {db_name}.{collection_name}")
    for msg in consumer:
        doc = msg.value
        try:
            col.insert_one(doc)
        except Exception:
            # duplicates etc.
            pass


if __name__ == "__main__":
    main()
