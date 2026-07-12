"""Pluggable ingestion sinks. `LocalFileSink` (JSON-lines on disk) is used
for local development and is also what the speed layer's Structured
Streaming file source reads from. `KinesisSink` is the AWS target and is
implemented but unexercised until AWS provisioning is approved -- same
producer code drives either one via `get_sink(settings.INGEST_SINK, ...)`.
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class Sink(ABC):
    @abstractmethod
    def write(self, records: list[dict]) -> int:
        """Write records, return the count actually written."""


class LocalFileSink(Sink):
    """Writes one JSON-lines file per poll cycle. A directory of small
    files (rather than one growing file) is what lets Spark Structured
    Streaming's file source detect new data incrementally."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._seq = 0

    def write(self, records: list[dict]) -> int:
        if not records:
            return 0
        # Windows' clock tick is coarse enough that two rapid writes can share
        # a strftime timestamp; the sequence number stops the second write
        # silently overwriting the first.
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        self._seq += 1
        path = self.directory / f"batch-{ts}-{self._seq:06d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        return len(records)


class KinesisSink(Sink):
    """AWS ingestion target. Not exercised until AWS provisioning is
    explicitly approved (see infra/runbook.md)."""

    def __init__(self, stream_name: str, region: str):
        import boto3

        self.client = boto3.client("kinesis", region_name=region)
        self.stream_name = stream_name

    def write(self, records: list[dict]) -> int:
        if not records:
            return 0
        written = 0
        for i in range(0, len(records), 500):  # put_records max batch size
            chunk = records[i : i + 500]
            entries = [
                {
                    "Data": (json.dumps(r) + "\n").encode("utf-8"),
                    "PartitionKey": str(r.get("city") or r.get("location_id") or "unknown"),
                }
                for r in chunk
            ]
            resp = self.client.put_records(StreamName=self.stream_name, Records=entries)
            failed = resp.get("FailedRecordCount", 0)
            written += len(chunk) - failed
            if failed:
                logger.warning("%d/%d records failed to put to Kinesis stream %s", failed, len(chunk), self.stream_name)
        return written


def get_sink(kind: str, **kwargs) -> Sink:
    if kind == "local":
        return LocalFileSink(kwargs["directory"])
    if kind == "kinesis":
        return KinesisSink(kwargs["stream_name"], kwargs["region"])
    raise ValueError(f"Unknown sink kind: {kind!r} (expected 'local' or 'kinesis')")
