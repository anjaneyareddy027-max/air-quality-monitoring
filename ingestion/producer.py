"""Ingestion producer: polls OpenAQ for the latest reading at each tracked
station, computes an EPA AQI sub-index per pollutant reading, and writes
normalized records to the configured sink every poll_interval_seconds.

Run from the project root as a module so `config`/`common`/`ingestion` are
importable without sys.path hacks:

    python -m ingestion.producer          # runs forever, one poll per interval
    python -m ingestion.producer --once   # single poll cycle, then exit (testing)
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import yaml

from common.aqi import sub_index
from common.schema import Reading
from config import settings
from ingestion.openaq_client import OpenAQClient
from ingestion.sinks import get_sink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def load_cities_config() -> dict:
    with open(settings.CITIES_CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def discover_locations(client: OpenAQClient, cfg: dict) -> list[dict]:
    """Resolve config/cities.yaml's country list into concrete OpenAQ
    stations, caching the result so restarts don't re-hit /locations every
    time (station lists change slowly relative to the poll interval)."""
    cache_file = settings.LOCATIONS_CACHE_FILE
    refresh_hours = cfg.get("discovery_refresh_hours", 24)
    if cache_file.exists():
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < refresh_hours:
            with open(cache_file, encoding="utf-8") as f:
                logger.info("Using cached station list (%.1fh old)", age_hours)
                return json.load(f)

    max_locations = cfg.get("max_locations", 150)
    page_size = cfg.get("locations_per_page", 100)
    locations: list[dict] = []
    for country in cfg.get("countries", []):
        if len(locations) >= max_locations:
            break
        iso = country["iso"]
        limit = min(country.get("limit", 10), max_locations - len(locations))
        try:
            found = client.list_locations(iso=iso, limit=limit, page_size=page_size)
        except Exception:
            logger.exception("Failed to discover locations for %s", iso)
            continue
        logger.info("Discovered %d station(s) in %s", len(found), country.get("label", iso))
        locations.extend(found)

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(locations, f)
    return locations


def _sensor_lookup(location: dict) -> dict:
    """sensorsId -> (pollutant_name, units) for one /locations entry."""
    lookup = {}
    for sensor in location.get("sensors", []):
        param = sensor.get("parameter") or {}
        name = (param.get("name") or "").lower()
        if name:
            lookup[sensor["id"]] = (name, param.get("units", ""))
    return lookup


def poll_once(client: OpenAQClient, locations: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for location in locations:
        loc_id = location.get("id")
        sensor_lookup = _sensor_lookup(location)
        city = location.get("locality") or location.get("name") or "Unknown"
        country = (location.get("country") or {}).get("code", "")
        coords = location.get("coordinates") or {}

        try:
            latest = client.get_latest(loc_id)
        except Exception:
            logger.exception("Failed to fetch latest readings for location %s", loc_id)
            continue

        for entry in latest:
            pollutant, unit = sensor_lookup.get(entry.get("sensorsId"), (None, None))
            value = entry.get("value")
            if pollutant is None or value is None:
                continue
            dt_utc = (entry.get("datetime") or {}).get("utc") or now
            reading = Reading(
                location_id=loc_id,
                location_name=location.get("name") or "",
                city=city,
                country=country,
                latitude=coords.get("latitude"),
                longitude=coords.get("longitude"),
                pollutant=pollutant,
                value=value,
                unit=unit,
                datetime_utc=dt_utc,
                ingested_at=now,
                aqi_sub_index=sub_index(pollutant, value, unit),
            )
            records.append(reading.to_json_dict())
    return records


def run(poll_once_only: bool = False) -> list[dict]:
    settings.ensure_dirs()
    if not settings.OPENAQ_API_KEY:
        raise SystemExit("OPENAQ_API_KEY is not set. Copy .env.example to .env and add your key.")

    cfg = load_cities_config()
    client = OpenAQClient(api_key=settings.OPENAQ_API_KEY, base_url=settings.OPENAQ_BASE_URL)
    sink = get_sink(
        settings.INGEST_SINK,
        directory=settings.RAW_DATA_DIR,
        stream_name=settings.KINESIS_STREAM_NAME,
        region=settings.AWS_REGION,
    )

    locations = discover_locations(client, cfg)
    logger.info("Tracking %d station(s)", len(locations))
    if not locations:
        logger.warning("No stations discovered - check config/cities.yaml and your API key/quota")

    interval = cfg.get("poll_interval_seconds", 300)
    records: list[dict] = []
    while True:
        started = time.monotonic()
        records = poll_once(client, locations)
        written = sink.write(records)
        dangerous = sum(1 for r in records if (r.get("aqi_sub_index") or 0) >= 151)
        logger.info(
            "Polled %d station(s) -> %d reading(s) -> %d written (%d dangerous-level readings)",
            len(locations), len(records), written, dangerous,
        )
        if poll_once_only:
            return records
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval - elapsed))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenAQ ingestion producer")
    parser.add_argument("--once", action="store_true", help="Poll a single cycle and exit (for testing)")
    args = parser.parse_args()
    run(poll_once_only=args.once)
