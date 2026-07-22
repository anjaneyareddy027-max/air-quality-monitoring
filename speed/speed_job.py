"""Speed layer: PySpark Structured Streaming with a 60-minute SLIDING window
(slide=5 min) per city, continuously answering the real-time question --
"which cities have dangerous AQI in the last hour?" -- with low latency as
new readings arrive, trading strict correctness (the batch layer's job) for
freshness.

Locally this watches the same JSON-lines directory the ingestion producer
writes to (Spark's file-source streaming trigger). On AWS the source swaps
to Kinesis (EMR's Kinesis-Spark connector) or Kafka -- the windowing/AQI
logic in `build_windowed_aqi` is unchanged either way (see infra/runbook.md).

`outputMode("update")` means a window's aggregate is re-emitted every time a
new reading lands in it -- no waiting for the watermark to fully close the
window -- which is what makes this "low-latency incremental views" rather
than a delayed append-only batch.

Run locally:
    python -m speed.speed_job                       # runs until Ctrl+C
    python -m speed.speed_job --run-seconds 60       # bounded run, for testing/demo
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.aqi import DANGEROUS_AQI_THRESHOLD
from common.schema import spark_schema
from config import settings

WINDOW_DURATION = "60 minutes"
SLIDE_DURATION = "5 minutes"
WATERMARK = "2 minutes"


def build_spark(app_name: str = "air-quality-speed", master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def build_windowed_aqi(raw_stream_df):
    """raw_stream_df: streaming DataFrame matching common.schema.spark_schema().
    Returns the windowed, low-latency rolling-AQI-per-city stream."""
    events = raw_stream_df.withColumn("event_time", F.to_timestamp("datetime_utc")).filter(
        F.col("aqi_sub_index").isNotNull()
    )

    windowed = (
        events.withWatermark("event_time", WATERMARK)
        .groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            "city",
            "country",
        )
        .agg(
            F.max("aqi_sub_index").alias("aqi"),
            F.max_by("pollutant", "aqi_sub_index").alias("dominant_pollutant"),
            F.count("*").alias("reading_count"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.first("longitude", ignorenulls=True).alias("longitude"),
        )
        .withColumn("dangerous", F.col("aqi") >= DANGEROUS_AQI_THRESHOLD)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city",
            "country",
            "aqi",
            "dominant_pollutant",
            "reading_count",
            "dangerous",
            "latitude",
            "longitude",
        )
    )
    return windowed


def _split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, prefix = without_scheme.partition("/")
    if not bucket:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, prefix.strip("/")


def _s3_key(prefix: str, name: str) -> str:
    return f"{prefix}/{name}" if prefix else name


def make_foreach_batch_writer(out_dir: str | Path):
    """Create a foreachBatch writer for either a local directory or S3.

    Local output keeps the original developer-friendly files:
    - dangerous_cities_now.json
    - window_history.jsonl

    S3 output overwrites the latest snapshot and writes immutable per-batch
    history files, since S3 objects cannot be appended to safely.
    """
    out_dir_str = str(out_dir)
    write_snapshot: Callable[[str], None]
    write_history: Callable[[int, str], None]

    if out_dir_str.startswith("s3://"):
        import boto3

        bucket, prefix = _split_s3_uri(out_dir_str)
        s3 = boto3.client("s3")

        def write_snapshot(text: str) -> None:
            s3.put_object(
                Bucket=bucket,
                Key=_s3_key(prefix, "dangerous_cities_now.json"),
                Body=text.encode("utf-8"),
                ContentType="application/json",
            )

        def write_history(batch_id: int, text: str) -> None:
            if not text:
                return
            s3.put_object(
                Bucket=bucket,
                Key=_s3_key(prefix, f"window_history/batch-{batch_id:012d}.jsonl"),
                Body=text.encode("utf-8"),
                ContentType="application/jsonlines",
            )

    else:
        local_out_dir = Path(out_dir_str)
        local_out_dir.mkdir(parents=True, exist_ok=True)
        history_path = local_out_dir / "window_history.jsonl"
        snapshot_path = local_out_dir / "dangerous_cities_now.json"

        def write_snapshot(text: str) -> None:
            snapshot_path.write_text(text, encoding="utf-8")

        def write_history(batch_id: int, text: str) -> None:
            if not text:
                return
            with history_path.open("a", encoding="utf-8") as f:
                f.write(text)

    def write_batch(micro_batch_df, batch_id: int) -> None:
        if micro_batch_df.rdd.isEmpty():
            return
        pdf = micro_batch_df.toPandas()

        # The dashboard only needs "as of now": for each city, the most
        # recent (rightmost) rolling 60-min window in this micro-batch.
        latest = pdf.sort_values("window_end").groupby(["city", "country"], as_index=False).tail(1)
        latest = latest.sort_values("aqi", ascending=False)
        latest["computed_at"] = datetime.now(timezone.utc).isoformat()

        write_snapshot(latest.to_json(orient="records", date_format="iso"))
        history_lines = "".join(json.dumps(row.to_dict(), default=str) + "\n" for _, row in pdf.iterrows())
        write_history(batch_id, history_lines)

        n_dangerous = int((latest["aqi"] >= DANGEROUS_AQI_THRESHOLD).sum())
        print(
            f"[speed_job] batch {batch_id}: {len(pdf)} window row(s) updated, "
            f"{len(latest)} city/ies tracked, {n_dangerous} dangerous right now"
        )

    return write_batch


def main():
    parser = argparse.ArgumentParser(description="Speed layer: 60-min sliding-window AQI per city")
    parser.add_argument("--raw-path", default=None, help="Streaming source dir (default: config RAW_DATA_DIR)")
    parser.add_argument("--out-path", default=None, help="Serving output dir (default: config SPEED_VIEW_DIR)")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--trigger-seconds", type=int, default=30, help="Micro-batch trigger interval")
    parser.add_argument("--max-files-per-trigger", type=int, default=None, help="Throttle files/trigger (benchmarking)")
    parser.add_argument("--run-seconds", type=int, default=0, help="Stop after N seconds (0 = run until Ctrl+C)")
    parser.add_argument("--checkpoint-path", default=None, help="Checkpoint path (use s3://... on EMR for restart safety)")
    args = parser.parse_args()

    settings.ensure_dirs()
    raw_path = args.raw_path or str(settings.RAW_DATA_DIR)
    out_path = args.out_path or str(settings.SPEED_VIEW_DIR)
    checkpoint = args.checkpoint_path or str(settings.CHECKPOINT_DIR / "speed_job")

    spark = build_spark(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

    reader = spark.readStream.schema(spark_schema()).option("recursiveFileLookup", "true")
    if args.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", args.max_files_per_trigger)
    raw_stream = reader.json(raw_path)

    windowed = build_windowed_aqi(raw_stream)

    query = (
        windowed.writeStream.outputMode("update")
        .foreachBatch(make_foreach_batch_writer(out_path))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    print(f"[speed_job] streaming from {raw_path} -> {out_path} (trigger={args.trigger_seconds}s)")
    if args.run_seconds:
        time.sleep(args.run_seconds)
        query.stop()
    else:
        query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
