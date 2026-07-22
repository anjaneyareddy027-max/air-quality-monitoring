"""Batch layer: computes accurate, comprehensive AQI views over the FULL
history of ingested readings using PySpark's data-parallel DataFrame engine
(a declarative MapReduce: groupBy+agg compiles to a shuffle + partition-local
reduce -- the same shuffle+reduce shape as Hadoop MapReduce, but scheduled
across all available cores/executors by Spark's DAG scheduler).

This is where the EPA methodology is applied properly: every reading in an
hour contributes to that hour's max-sub-index AQI, not just the single most
recent reading the speed layer sees.

Run locally:
    python -m batch.batch_job                 # parallel, local[*]
    python -m batch.batch_job --master local[1]   # sequential, for benchmarking
    python -m batch.batch_job --repeat-seconds 900 # refresh every 15 minutes

On AWS this reads from s3://<bucket>/raw/ and writes to
s3://<bucket>/batch-view/ instead of the local data/ directories (see
infra/runbook.md) via --raw-path / --out-path.
"""

import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.aqi import DANGEROUS_AQI_THRESHOLD
from common.schema import spark_schema
from config import settings


def build_spark(app_name: str = "air-quality-batch", master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def run_batch(spark: SparkSession, raw_path: str, batch_view_path: str):
    schema = spark_schema()
    raw = (
        spark.read.schema(schema).option("recursiveFileLookup", "true").json(raw_path)
        .withColumn("event_time", F.to_timestamp("datetime_utc"))
        .withColumn("hour", F.date_trunc("hour", F.col("event_time")))
        .filter(F.col("aqi_sub_index").isNotNull())
    )

    # Per (city, hour): EPA max-sub-index rule across every pollutant reading
    # observed that hour -> the accurate historical AQI for that city-hour.
    hourly = (
        raw.groupBy("city", "country", "hour")
        .agg(
            F.max_by("pollutant", "aqi_sub_index").alias("dominant_pollutant"),
            F.max("aqi_sub_index").alias("aqi"),
            F.count("*").alias("reading_count"),
        )
        .withColumn("dangerous", F.col("aqi") >= DANGEROUS_AQI_THRESHOLD)
    )

    hourly.write.mode("overwrite").partitionBy("country").parquet(f"{batch_view_path}/hourly_city_aqi")

    # Per-city rollup over the full accumulated history (the "top N worst
    # cities" style summary, and the batch baseline the serving layer merges
    # with the speed layer's real-time view).
    summary = (
        hourly.groupBy("city", "country")
        .agg(
            F.count("*").alias("hours_observed"),
            F.avg("aqi").alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.max_by("hour", "aqi").alias("max_aqi_at"),
            F.max_by("dominant_pollutant", "aqi").alias("worst_pollutant"),
            (F.sum(F.col("dangerous").cast("int")) / F.count("*")).alias("pct_hours_dangerous"),
        )
        .orderBy(F.desc("max_aqi"))
    )

    summary.write.mode("overwrite").parquet(f"{batch_view_path}/city_summary")
    return hourly, summary


def main():
    parser = argparse.ArgumentParser(description="Batch layer: full-history per-city AQI aggregates")
    parser.add_argument("--raw-path", default=None, help="Raw JSON-lines directory/glob (default: config RAW_DATA_DIR)")
    parser.add_argument("--out-path", default=None, help="Batch-view output path (default: config BATCH_VIEW_DIR)")
    parser.add_argument("--master", default="local[*]", help="Spark master, e.g. local[1] for sequential benchmark runs")
    parser.add_argument(
        "--repeat-seconds",
        type=int,
        default=0,
        help="Repeat the full-history refresh every N seconds (0 = run once)",
    )
    args = parser.parse_args()

    settings.ensure_dirs()
    raw_path = args.raw_path or str(settings.RAW_DATA_DIR)
    out_path = args.out_path or str(settings.BATCH_VIEW_DIR)

    if raw_path.startswith(str(settings.RAW_DATA_DIR)) and not any(Path(raw_path).glob("*.jsonl")):
        raise SystemExit(
            f"No .jsonl files found under {raw_path} yet. Run `python -m ingestion.producer --once` first."
        )

    spark = build_spark(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

    while True:
        started = time.perf_counter()
        hourly, summary = run_batch(spark, raw_path, out_path)
        hourly_count = hourly.count()
        elapsed = time.perf_counter() - started

        print(f"[batch_job] {hourly_count} city-hour row(s) written in {elapsed:.2f}s (master={args.master})")
        print("[batch_job] Worst city-hours in the accumulated history:")
        summary.show(10, truncate=False)

        if args.repeat_seconds <= 0:
            break

        sleep_for = max(0, args.repeat_seconds - elapsed)
        print(f"[batch_job] next full-history refresh in {sleep_for:.0f}s")
        time.sleep(sleep_for)

    spark.stop()


if __name__ == "__main__":
    main()
