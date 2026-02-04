from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, count, avg

import yaml
from pathlib import Path


def load_cfg() -> dict:
    cfg_path = Path(__file__).resolve().parents[2] / "config.yaml"
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def main() -> None:
    cfg = load_cfg()
    input_csv = f"{cfg['paths']['processed_dir']}/features_daily.csv"
    out_dir = f"{cfg['paths']['processed_dir']}/spark_outputs_monthly"

    spark = SparkSession.builder.appName("bigdata-tsla-spark-batch").getOrCreate()
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)

    # your features file uses a day column (from merge step)
    df = df.withColumn("d", to_date(col("day")))

    # labels in config: modeling.target = direction_next_day
    # labels in config: modeling.target, but your CSV column is direction_next
    cfg_target = cfg["modeling"]["target"]
    label_col = cfg_target if cfg_target in df.columns else "direction_next"


    monthly = (
        df.groupBy(year("d").alias("year"), month("d").alias("month"), col(label_col).alias("label"))
        .agg(
            count("*").alias("n_days"),
            avg(col("mean_sentiment")).alias("avg_sentiment") if "mean_sentiment" in df.columns else avg(col("Close")).alias("avg_close"),
            avg(col("sum_influence")).alias("avg_influence") if "sum_influence" in df.columns else avg(col("mean_score")).alias("avg_score"),
        )
        .orderBy("year", "month", "label")
    )

    monthly.write.mode("overwrite").parquet(f"{out_dir}/monthly_label_stats.parquet")
    monthly.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/monthly_label_stats_csv")

    spark.stop()
    print(f"[spark] wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
