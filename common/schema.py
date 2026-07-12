"""Shared record shape produced by ingestion and consumed by both the batch
and speed layers. One record = one pollutant reading at one location at one
point in time (denormalized/flattened, not nested) so that both Spark jobs
can do a plain groupBy + max_by(aqi_sub_index) to get the EPA "worst
pollutant wins" overall AQI per city per window -- no per-record multi
-pollutant reconstruction needed downstream.

Kept free of any PySpark import so the ingestion producer (which runs far
more often than the Spark jobs and shouldn't need a JVM/JAVA_HOME) can import
it cheaply. Spark jobs call `spark_schema()` to get the matching StructType.
"""

from dataclasses import asdict, dataclass
from typing import Optional

FIELDS = (
    "location_id",
    "location_name",
    "city",
    "country",
    "latitude",
    "longitude",
    "pollutant",
    "value",
    "unit",
    "datetime_utc",
    "ingested_at",
    "aqi_sub_index",
)


@dataclass
class Reading:
    location_id: int
    location_name: str
    city: str
    country: str
    latitude: Optional[float]
    longitude: Optional[float]
    pollutant: str
    value: float
    unit: str
    datetime_utc: str  # ISO 8601 UTC, e.g. "2026-07-02T13:00:00Z"
    ingested_at: str  # ISO 8601 UTC, when the producer polled this reading
    aqi_sub_index: Optional[float]  # EPA sub-index for this single pollutant

    def to_json_dict(self) -> dict:
        return asdict(self)


def spark_schema():
    """Explicit StructType matching `Reading`, for both the batch job (JSON
    file read) and the speed job (Structured Streaming file/Kinesis source,
    which requires a declared schema rather than inference)."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("location_id", IntegerType(), True),
            StructField("location_name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("pollutant", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("datetime_utc", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("aqi_sub_index", DoubleType(), True),
        ]
    )
