# Real-time streaming (Kafka + Spark + Mongo)

This repo supports a real-time Reddit pipeline:

- Producer (Reddit): `scripts/run_stream_reddit_producer.py` → Kafka topic `kafka.topic_reddit`
- Consumer (Reddit): `integrations/spark/spark_stream_reddit_to_mongo.py` → MongoDB collections
  - `social_raw` (raw + sentiment + influence per post)
  - `features` (daily aggregate per day)
- Producer (Price): `scripts/run_stream_price_producer.py` → Kafka topic `kafka.topic_price`
- Consumer (Price): `integrations/spark/spark_stream_price_to_mongo.py` → MongoDB `price_raw`
- Refresh: `scripts/run_streaming_refresh.py` updates `data/processed/features_daily.csv`
  and `data/processed/predictions_daily.csv` for the dashboard.

## Prereqs
- Docker (for Kafka + Mongo)
- Spark 3.5.x (for `spark-submit`)
- Python deps from `requirements.txt`

## Quick run (3 terminals)

Terminal A (infra):
```
docker-compose up -d
```

Terminal B (producer):
```
python -m scripts.run_stream_reddit_producer
```

Terminal C (consumer):
```
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  integrations/spark/spark_stream_reddit_to_mongo.py
```

Terminal D (producer):
```
python -m scripts.run_stream_price_producer
```

Terminal E (consumer):
```
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  integrations/spark/spark_stream_price_to_mongo.py
```

Terminal F (refresh loop):
```
python -m scripts.run_streaming_refresh
```

## Single command
```
./scripts/run_streaming_all.sh
```

## Notes
- Edit `config.yaml` to change subreddits/keywords or Kafka bootstrap.
- MongoDB database/collection names are in `config.yaml` under `mongo`.
- If you hit `ModuleNotFoundError: No module named 'src'`, ensure you run from the repo root with `PYTHONPATH=.`.
