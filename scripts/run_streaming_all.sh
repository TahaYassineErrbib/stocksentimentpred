#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "Warning: no virtualenv detected. Activate .venv before running."
fi

LOG_DIR="logs/streaming"
mkdir -p "$LOG_DIR"

echo "[all] starting docker services..."
docker compose up -d

echo "[all] starting reddit producer..."
nohup python -m scripts.run_stream_reddit_producer > "$LOG_DIR/reddit_producer.log" 2>&1 &
echo $! > "$LOG_DIR/reddit_producer.pid"

echo "[all] starting reddit spark consumer..."
nohup env PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  integrations/spark/spark_stream_reddit_to_mongo.py \
  > "$LOG_DIR/reddit_spark.log" 2>&1 &
echo $! > "$LOG_DIR/reddit_spark.pid"

echo "[all] starting price producer..."
nohup python -m scripts.run_stream_price_producer > "$LOG_DIR/price_producer.log" 2>&1 &
echo $! > "$LOG_DIR/price_producer.pid"

echo "[all] starting price spark consumer..."
nohup env PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  integrations/spark/spark_stream_price_to_mongo.py \
  > "$LOG_DIR/price_spark.log" 2>&1 &
echo $! > "$LOG_DIR/price_spark.pid"

echo "[all] starting streaming refresh (features + predictions)..."
nohup python -m scripts.run_streaming_refresh > "$LOG_DIR/refresh.log" 2>&1 &
echo $! > "$LOG_DIR/refresh.pid"

echo "[all] done. Logs in $LOG_DIR"
