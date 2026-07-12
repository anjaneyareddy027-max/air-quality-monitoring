"""US EPA Air Quality Index (AQI) calculation.

Breakpoints are the official EPA table effective 6 May 2024 (40 CFR Part 58,
Appendix G / AQS breakpoints), sourced from
https://aqs.epa.gov/aqsweb/documents/codetables/aqi_breakpoints.html

The EPA breakpoints are defined against specific averaging periods (24-hr for
PM2.5/PM10, 8-hr for O3/CO, 1-hr for NO2/SO2). OpenAQ exposes near-hourly
sensor snapshots rather than true rolling averages, so treating a single
recent reading (or a short rolling mean of the last hour, see speed/speed_job.py)
as if it were the averaging-period value is a deliberate, documented
approximation -- the same one most consumer "real-time AQI" apps make. The
batch layer is where a true rolling-average AQI over full history belongs.
"""

from dataclasses import dataclass, field
from typing import Optional

# AQI >= 151 ("Unhealthy" or worse) is treated as dangerous for this project.
DANGEROUS_AQI_THRESHOLD = 151

# Standard EPA molar-volume conversion (25 C, 1 atm) for gases reported in
# mass concentration (ug/m3) instead of the breakpoint table's volumetric unit.
_MOLAR_VOLUME_25C = 24.45  # L/mol
_MOLECULAR_WEIGHT = {
    "o3": 48.00,
    "co": 28.01,
    "no2": 46.0055,
    "so2": 64.066,
}

# pollutant -> (breakpoint unit, decimal places to truncate concentration to)
_BREAKPOINT_UNIT = {
    "pm25": ("ugm3", 1),
    "pm10": ("ugm3", 0),
    "o3": ("ppm", 3),
    "co": ("ppm", 1),
    "so2": ("ppb", 0),
    "no2": ("ppb", 0),
}

# pollutant -> [(c_lo, c_hi, aqi_lo, aqi_hi), ...] in the units above
BREAKPOINTS = {
    "pm25": [
        (0.0, 9.0, 0, 50),
        (9.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 125.4, 151, 200),
        (125.5, 225.4, 201, 300),
        (225.5, 325.4, 301, 500),
    ],
    "pm10": [
        (0, 54, 0, 50),
        (55, 154, 51, 100),
        (155, 254, 101, 150),
        (255, 354, 151, 200),
        (355, 424, 201, 300),
        (425, 604, 301, 500),
    ],
    # 8-hr O3, ppm. EPA only defines 301-500 via the 1-hr metric; out of
    # scope here since it requires a second averaging period.
    "o3": [
        (0.000, 0.054, 0, 50),
        (0.055, 0.070, 51, 100),
        (0.071, 0.085, 101, 150),
        (0.086, 0.105, 151, 200),
        (0.106, 0.200, 201, 300),
    ],
    "co": [
        (0.0, 4.4, 0, 50),
        (4.5, 9.4, 51, 100),
        (9.5, 12.4, 101, 150),
        (12.5, 15.4, 151, 200),
        (15.5, 30.4, 201, 300),
        (30.5, 50.4, 301, 500),
    ],
    "so2": [
        (0, 35, 0, 50),
        (36, 75, 51, 100),
        (76, 185, 101, 150),
        (186, 304, 151, 200),
        (305, 604, 201, 300),
        (605, 1004, 301, 500),
    ],
    "no2": [
        (0, 53, 0, 50),
        (54, 100, 51, 100),
        (101, 360, 101, 150),
        (361, 649, 151, 200),
        (650, 1249, 201, 300),
        (1250, 2049, 301, 500),
    ],
}

_CATEGORIES = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]


def aqi_category(aqi: float) -> str:
    for lo, hi, name in _CATEGORIES:
        if lo <= aqi <= hi:
            return name
    return "Hazardous" if aqi > 500 else "Good"


def is_dangerous(aqi: float) -> bool:
    return aqi >= DANGEROUS_AQI_THRESHOLD


def _truncate(value: float, decimals: int) -> float:
    factor = 10**decimals
    return int(value * factor) / factor


def _normalize_unit(pollutant: str, value: float, unit: str) -> Optional[float]:
    """Convert `value` (in `unit`, as reported by OpenAQ) into the unit the
    breakpoint table for `pollutant` expects. Returns None if the pollutant
    or unit combination isn't supported."""
    target_unit, decimals = _BREAKPOINT_UNIT.get(pollutant, (None, None))
    if target_unit is None:
        return None
    unit = unit.lower().replace("µ", "u").replace("g/m³", "g/m3")

    if pollutant in ("pm25", "pm10"):
        if unit in ("ug/m3", "ugm3"):
            return _truncate(value, decimals)
        return None

    # Gaseous pollutants: breakpoints want ppm (o3, co) or ppb (so2, no2).
    if unit in ("ppm",):
        ppm = value
    elif unit in ("ppb",):
        ppm = value / 1000.0
    elif unit in ("ug/m3", "ugm3"):
        mw = _MOLECULAR_WEIGHT[pollutant]
        ppm = (value * _MOLAR_VOLUME_25C) / (mw * 1000.0)
    else:
        return None

    result = ppm if target_unit == "ppm" else ppm * 1000.0
    return _truncate(result, decimals)


def sub_index(pollutant: str, value: float, unit: str) -> Optional[float]:
    """Linear-interpolated AQI sub-index for a single pollutant reading, or
    None if the pollutant/unit/value is outside anything we can score."""
    pollutant = pollutant.lower()
    table = BREAKPOINTS.get(pollutant)
    if table is None or value is None:
        return None

    concentration = _normalize_unit(pollutant, value, unit)
    if concentration is None or concentration < 0:
        return None

    for c_lo, c_hi, aqi_lo, aqi_hi in table:
        if c_lo <= concentration <= c_hi:
            return ((aqi_hi - aqi_lo) / (c_hi - c_lo)) * (concentration - c_lo) + aqi_lo

    # Above the top breakpoint: clamp to the worst category's ceiling rather
    # than silently dropping a hazardous reading.
    c_lo, c_hi, aqi_lo, aqi_hi = table[-1]
    if concentration > c_hi:
        return float(aqi_hi)
    return None


@dataclass
class AQIResult:
    aqi: float
    category: str
    dominant_pollutant: str
    dangerous: bool
    sub_indices: dict = field(default_factory=dict)


def compute_aqi(readings: dict) -> Optional[AQIResult]:
    """readings: {pollutant: (value, unit)}. Overall AQI is the EPA
    "max sub-index" rule: the worst-scoring pollutant determines the AQI and
    is reported as the dominant pollutant."""
    sub_indices = {}
    for pollutant, (value, unit) in readings.items():
        idx = sub_index(pollutant, value, unit)
        if idx is not None:
            sub_indices[pollutant] = idx

    if not sub_indices:
        return None

    dominant_pollutant, aqi = max(sub_indices.items(), key=lambda kv: kv[1])
    aqi = round(aqi)
    return AQIResult(
        aqi=aqi,
        category=aqi_category(aqi),
        dominant_pollutant=dominant_pollutant,
        dangerous=is_dangerous(aqi),
        sub_indices=sub_indices,
    )
