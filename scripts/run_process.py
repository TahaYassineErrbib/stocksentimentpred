from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from src.utils.config import load_config
from src.utils.mongo import get_collection, bulk_upsert_by_key
from src.processing.sentiment import score_text
from src.processing.influence_index import social_influence_index


def day_key(iso_ts: str) -> str:
    # iso_ts is like "2026-02-04T00:00:00+00:00"
    dt = pd.to_datetime(iso_ts, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def main() -> None:
    cfg = load_config("config.yaml")

    mongo_uri = cfg["mongo"]["uri"]
    db = cfg["mongo"]["db"]

    social_raw_col = get_collection(mongo_uri, db, cfg["mongo"]["collections"]["social_raw"])
    features_col = get_collection(mongo_uri, db, cfg["mongo"]["collections"]["features"])

    out_path = Path(cfg["paths"]["processed_dir"]) / "sentiment_daily.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Pull recent docs (you can remove the limit later)
    cursor = social_raw_col.find({}, {"_id": 1, "created_iso": 1, "text": 1, "score": 1, "subreddit": 1})
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        created_iso = doc.get("created_iso")
        if not created_iso and doc.get("created_utc") is not None:
            # derive ISO from epoch seconds
            dt = datetime.fromtimestamp(int(doc["created_utc"]), tz=timezone.utc)
            created_iso = dt.isoformat()

        if not created_iso:
            continue
        text = doc.get("text", "")
        s = score_text(text)
        infl = social_influence_index(s["sentiment_avg"], int(doc.get("score") or 0))
        rows.append(
            {
                "_id": doc["_id"],
                "day": day_key(doc["created_iso"]),
                "subreddit": doc.get("subreddit"),
                "score": int(doc.get("score") or 0),
                "vader_compound": s["vader_compound"],
                "textblob_polarity": s["textblob_polarity"],
                "sentiment_avg": s["sentiment_avg"],
                "influence_index": infl,
            }
        )

    if not rows:
        print("No social_raw docs found to process.")
        return

    df = pd.DataFrame(rows).dropna(subset=["day"])

    # Daily aggregation (overall)
    daily = (
        df.groupby("day")
        .agg(
            post_count=("day", "size"),
            mean_sentiment=("sentiment_avg", "mean"),
            mean_influence=("influence_index", "mean"),
            sum_influence=("influence_index", "sum"),
            mean_score=("score", "mean"),
        )
        .reset_index()
        .sort_values("day")
    )

    daily.to_csv(out_path, index=False)

    # Upsert into Mongo features collection
    feature_docs = []
    for _, r in daily.iterrows():
        d = {
            "_id": f"social_{r['day']}",
            "day": r["day"],
            "source": "reddit",
            "post_count": int(r["post_count"]),
            "mean_sentiment": float(r["mean_sentiment"]),
            "mean_influence": float(r["mean_influence"]),
            "sum_influence": float(r["sum_influence"]),
            "mean_score": float(r["mean_score"]),
        }
        feature_docs.append(d)

    upserts = bulk_upsert_by_key(features_col, feature_docs, key_field="_id")

    print("=== Social processing done ===")
    print(f"posts_scored: {len(df)}")
    print(f"days: {len(daily)}")
    print(f"csv: {out_path.as_posix()}")
    print(f"mongo_upserts: {upserts}")


if __name__ == "__main__":
    main()
