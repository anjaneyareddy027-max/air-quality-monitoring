"""Serving layer: merges the batch layer's historical baseline
(city_summary parquet) with the speed layer's real-time rolling-hour view
(dangerous_cities_now.json) into one queryable table -- the Lambda
architecture's defining "merge" step.

Locally this writes data/serving-view/serving_view.json for the dashboard.
On AWS the same merge logic targets DynamoDB (point lookups by city) and
S3+Athena (ad-hoc SQL) instead of local files (see infra/runbook.md).
"""

from pathlib import Path

import pandas as pd

from config import settings

_SPEED_COLUMNS = [
    "city", "country", "aqi", "dominant_pollutant", "reading_count",
    "dangerous", "latitude", "longitude", "window_start", "window_end", "computed_at",
]
_SERVING_COLUMNS = _SPEED_COLUMNS + [
    "historical_avg_aqi", "historical_max_aqi", "historical_worst_pollutant",
    "pct_hours_dangerous", "vs_historical_avg",
]


def load_batch_summary(batch_view_dir: Path) -> pd.DataFrame:
    path = Path(batch_view_dir) / "city_summary"
    if not path.exists():
        return pd.DataFrame(columns=["city", "country", "avg_aqi", "max_aqi", "worst_pollutant", "pct_hours_dangerous"])
    return pd.read_parquet(path)


def load_speed_snapshot(speed_view_dir: Path) -> pd.DataFrame:
    path = Path(speed_view_dir) / "dangerous_cities_now.json"
    if not path.exists():
        return pd.DataFrame(columns=_SPEED_COLUMNS)
    return pd.read_json(path)


def merge_views(batch_summary: pd.DataFrame, speed_snapshot: pd.DataFrame) -> pd.DataFrame:
    if speed_snapshot.empty:
        return pd.DataFrame(columns=_SERVING_COLUMNS)

    baseline = batch_summary.rename(
        columns={
            "avg_aqi": "historical_avg_aqi",
            "max_aqi": "historical_max_aqi",
            "worst_pollutant": "historical_worst_pollutant",
        }
    )
    keep = [c for c in ["city", "country", "historical_avg_aqi", "historical_max_aqi", "historical_worst_pollutant", "pct_hours_dangerous"] if c in baseline.columns]

    merged = speed_snapshot.merge(baseline[keep], on=["city", "country"], how="left")
    merged["vs_historical_avg"] = merged["aqi"] - merged.get("historical_avg_aqi")
    return merged.sort_values("aqi", ascending=False).reset_index(drop=True)


def dangerous_cities_now(serving_df: pd.DataFrame) -> pd.DataFrame:
    if serving_df.empty or "dangerous" not in serving_df.columns:
        return serving_df
    return serving_df[serving_df["dangerous"].astype(bool)].sort_values("aqi", ascending=False)


def build_and_write_serving_view() -> pd.DataFrame:
    settings.ensure_dirs()
    batch_summary = load_batch_summary(settings.BATCH_VIEW_DIR)
    speed_snapshot = load_speed_snapshot(settings.SPEED_VIEW_DIR)
    merged = merge_views(batch_summary, speed_snapshot)

    out_dir = Path(settings.SERVING_VIEW_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    merged.to_json(out_dir / "serving_view.json", orient="records", date_format="iso")
    return merged


if __name__ == "__main__":
    merged = build_and_write_serving_view()
    dangerous = dangerous_cities_now(merged)
    print(f"[serving.merge] {len(merged)} city/ies in serving view, {len(dangerous)} dangerous right now")
    if not dangerous.empty:
        print(dangerous[["city", "country", "aqi", "dominant_pollutant"]].to_string(index=False))
