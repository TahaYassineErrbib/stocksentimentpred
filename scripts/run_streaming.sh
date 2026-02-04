#!/usr/bin/env bash
set -euo pipefail

# Helper to run the Reddit -> Kafka -> Spark -> Mongo streaming path.
# Run each command in its own terminal for easier logs and control.

echo "1) Start infra (Kafka + Mongo) with Docker:"
echo "   docker-compose up -d"
echo
echo "2) Run the Reddit Kafka producer:"
echo "   python -m scripts.run_stream_reddit_producer"
echo
echo "3) Run the Spark Structured Streaming consumer (Reddit -> Mongo + CSV):"
echo "   PYTHONPATH=. spark-submit \\"
echo "     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "     integrations/spark/spark_stream_reddit_to_mongo.py"
echo
echo "4) Run the Price Kafka producer:"
echo "   python -m scripts.run_stream_price_producer"
echo
echo "5) Run the Spark Structured Streaming consumer (Price -> Mongo):"
echo "   PYTHONPATH=. spark-submit \\"
echo "     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "     integrations/spark/spark_stream_price_to_mongo.py"
echo
echo "6) Run streaming refresh (features + predictions):"
echo "   python -m scripts.run_streaming_refresh"
echo
echo "Note: Spark must be on PATH (spark-submit)."
