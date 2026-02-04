from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

from src.utils.config import load_config
from src.utils.mongo import get_collection, bulk_upsert_by_key


def _upsert_price_raw(rows: List[Dict[str, Any]], mongo_uri: str, db: str, coll: str) -> int:
    col = get_collection(mongo_uri, db, coll)
    docs = []
    for r in rows:
        if not r.get("ticker") or not r.get("timestamp"):
            continue
        d = dict(r)
        d["_id"] = f'{r["ticker"]}_{r["timestamp"]}'
        docs.append(d)
    return bulk_upsert_by_key(col, docs, key_field="_id")


def main() -> None:
    cfg = load_config("config.yaml")

    spark = (
        SparkSession.builder.appName("bigdata-tsla-spark-stream-price")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("ticker", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("adj_close", DoubleType(), True),
            StructField("volume", LongType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap"])
        .option("subscribe", cfg["kafka"]["topic_price"])
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("j")).select("j.*")

    mongo_uri = cfg["mongo"]["uri"]
    mongo_db = cfg["mongo"]["db"]
    price_raw_coll = cfg["mongo"]["collections"]["price_raw"]

    def foreach_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        rows: List[Dict[str, Any]] = []
        for _, row in pdf.iterrows():
            rows.append(
                {
                    "ticker": row.get("ticker"),
                    "timestamp": row.get("timestamp"),
                    "open": float(row.get("open") or 0.0),
                    "high": float(row.get("high") or 0.0),
                    "low": float(row.get("low") or 0.0),
                    "close": float(row.get("close") or 0.0),
                    "adj_close": float(row.get("adj_close") or row.get("close") or 0.0),
                    "volume": int(row.get("volume") or 0),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        _upsert_price_raw(rows, mongo_uri, mongo_db, price_raw_coll)

    query = (
        parsed.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", "data/processed/spark_checkpoints/price_stream")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
