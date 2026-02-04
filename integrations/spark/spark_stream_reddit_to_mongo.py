from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import col, from_json

from src.utils.config import load_config
from src.utils.mongo import get_collection, bulk_upsert_by_key
from src.processing.sentiment import score_text
from src.processing.influence_index import social_influence_index


def day_key(iso_ts: str) -> str | None:
    dt = pd.to_datetime(iso_ts, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _upsert_social_raw(rows: List[Dict[str, Any]], mongo_uri: str, db: str, coll: str) -> int:
    col = get_collection(mongo_uri, db, coll)
    docs = [r for r in rows if r.get("_id")]
    return bulk_upsert_by_key(col, docs, key_field="_id")


def _upsert_features_daily(rows: List[Dict[str, Any]], mongo_uri: str, db: str, coll: str) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["day"])
    if df.empty:
        return 0

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
    )

    feature_docs = []
    for _, r in daily.iterrows():
        feature_docs.append(
            {
                "_id": f"social_{r['day']}",
                "day": r["day"],
                "source": "reddit_stream",
                "post_count": int(r["post_count"]),
                "mean_sentiment": float(r["mean_sentiment"]),
                "mean_influence": float(r["mean_influence"]),
                "sum_influence": float(r["sum_influence"]),
                "mean_score": float(r["mean_score"]),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    col = get_collection(mongo_uri, db, coll)
    return bulk_upsert_by_key(col, feature_docs, key_field="_id")


def _write_sentiment_daily_csv(rows: List[Dict[str, Any]], out_path: str) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["day"])
    if df.empty:
        return

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
    )

    try:
        existing = pd.read_csv(out_path)
        combined = pd.concat([existing, daily], ignore_index=True)
        combined["sum_sentiment"] = combined["mean_sentiment"] * combined["post_count"]
        combined["sum_score"] = combined["mean_score"] * combined["post_count"]
        combined["sum_influence_weighted"] = combined["mean_influence"] * combined["post_count"]
        combined = (
            combined.groupby("day", as_index=False)
            .agg(
                post_count=("post_count", "sum"),
                sum_sentiment=("sum_sentiment", "sum"),
                sum_influence=("sum_influence", "sum"),
                sum_influence_weighted=("sum_influence_weighted", "sum"),
                sum_score=("sum_score", "sum"),
            )
        )
        combined["mean_sentiment"] = combined["sum_sentiment"] / combined["post_count"]
        combined["mean_influence"] = combined["sum_influence_weighted"] / combined["post_count"]
        combined["mean_score"] = combined["sum_score"] / combined["post_count"]
        combined = combined.drop(columns=["sum_sentiment", "sum_influence_weighted", "sum_score"]).sort_values("day")
    except FileNotFoundError:
        combined = daily.sort_values("day")

    combined.to_csv(out_path, index=False)


def main() -> None:
    cfg = load_config("config.yaml")

    spark = (
        SparkSession.builder.appName("bigdata-tsla-spark-stream-reddit")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("created_utc", LongType(), True),
            StructField("created_iso", StringType(), True),
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("selftext", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("author", StringType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("permalink", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap"])
        .option("subscribe", cfg["kafka"]["topic_reddit"])
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("j")).select("j.*")

    mongo_uri = cfg["mongo"]["uri"]
    mongo_db = cfg["mongo"]["db"]
    social_raw_coll = cfg["mongo"]["collections"]["social_raw"]
    features_coll = cfg["mongo"]["collections"]["features"]
    out_csv = str(Path(cfg["paths"]["processed_dir"]) / "sentiment_daily.csv")

    def foreach_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        rows: List[Dict[str, Any]] = []
        for _, row in pdf.iterrows():
            text = row.get("text", "") or ""
            s = score_text(text)
            infl = social_influence_index(s["sentiment_avg"], int(row.get("score") or 0))
            created_iso = row.get("created_iso")
            if not created_iso and row.get("created_utc") is not None:
                created_iso = datetime.fromtimestamp(int(row["created_utc"]), tz=timezone.utc).isoformat()

            rows.append(
                {
                    "_id": row.get("_id"),
                    "type": row.get("type") or "post",
                    "source": row.get("source") or "reddit_stream",
                    "subreddit": row.get("subreddit"),
                    "created_utc": int(row.get("created_utc") or 0),
                    "created_iso": created_iso,
                    "title": row.get("title"),
                    "text": text,
                    "selftext": row.get("selftext"),
                    "score": int(row.get("score") or 0),
                    "author": row.get("author"),
                    "num_comments": int(row.get("num_comments") or 0),
                    "permalink": row.get("permalink"),
                    "url": row.get("url"),
                    "vader_compound": s["vader_compound"],
                    "textblob_polarity": s["textblob_polarity"],
                    "sentiment_avg": s["sentiment_avg"],
                    "influence_index": infl,
                    "day": day_key(created_iso) if created_iso else None,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        _upsert_social_raw(rows, mongo_uri, mongo_db, social_raw_coll)
        _upsert_features_daily(rows, mongo_uri, mongo_db, features_coll)
        _write_sentiment_daily_csv(rows, out_csv)

    query = (
        parsed.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", "data/processed/spark_checkpoints/reddit_stream")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
