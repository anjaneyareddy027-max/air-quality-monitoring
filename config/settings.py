"""Central config: env vars (.env) + paths, shared by ingestion, batch,
speed, serving and benchmark modules."""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
OPENAQ_BASE_URL = "https://api.openaq.org/v3"

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", ROOT_DIR / "data" / "raw"))
BATCH_VIEW_DIR = Path(os.getenv("BATCH_VIEW_DIR", ROOT_DIR / "data" / "batch-view"))
SPEED_VIEW_DIR = Path(os.getenv("SPEED_VIEW_DIR", ROOT_DIR / "data" / "speed-view"))
SERVING_VIEW_DIR = Path(os.getenv("SERVING_VIEW_DIR", ROOT_DIR / "data" / "serving-view"))
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", ROOT_DIR / "data" / "checkpoints"))
LOCATIONS_CACHE_FILE = ROOT_DIR / "data" / "locations_cache.json"
CITIES_CONFIG_FILE = ROOT_DIR / "config" / "cities.yaml"

INGEST_SINK = os.getenv("INGEST_SINK", "local")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "air-quality-raw")


def ensure_dirs() -> None:
    for d in (RAW_DATA_DIR, BATCH_VIEW_DIR, SPEED_VIEW_DIR, SERVING_VIEW_DIR, CHECKPOINT_DIR):
        d.mkdir(parents=True, exist_ok=True)
