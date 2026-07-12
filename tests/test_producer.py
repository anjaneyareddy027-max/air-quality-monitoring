from ingestion.producer import _sensor_lookup, poll_once


LOCATION = {
    "id": 8118,
    "name": "New Delhi US Embassy",
    "locality": "New Delhi",
    "country": {"code": "IN", "name": "India"},
    "coordinates": {"latitude": 28.59, "longitude": 77.19},
    "sensors": [
        {"id": 1, "parameter": {"name": "pm25", "units": "µg/m³"}},
        {"id": 2, "parameter": {"name": "o3", "units": "ppm"}},
        {"id": 3, "parameter": {}},  # sensor with no parameter info -> skipped
    ],
}


class FakeClient:
    def __init__(self, latest_by_location):
        self.latest_by_location = latest_by_location

    def get_latest(self, location_id):
        result = self.latest_by_location[location_id]
        if isinstance(result, Exception):
            raise result
        return result


def test_sensor_lookup_maps_id_to_pollutant_and_unit():
    lookup = _sensor_lookup(LOCATION)
    assert lookup == {1: ("pm25", "µg/m³"), 2: ("o3", "ppm")}


def test_poll_once_normalizes_and_scores_readings():
    client = FakeClient(
        {
            8118: [
                {"sensorsId": 1, "value": 120.0, "datetime": {"utc": "2026-07-02T10:00:00Z"}},
                {"sensorsId": 2, "value": 0.03, "datetime": {"utc": "2026-07-02T10:00:00Z"}},
                {"sensorsId": 99, "value": 5.0},  # unknown sensor -> dropped
                {"sensorsId": 1, "value": None},  # null value -> dropped
            ]
        }
    )

    records = poll_once(client, [LOCATION])

    assert len(records) == 2
    pm25 = next(r for r in records if r["pollutant"] == "pm25")
    assert pm25["city"] == "New Delhi"
    assert pm25["country"] == "IN"
    assert pm25["latitude"] == 28.59
    assert pm25["datetime_utc"] == "2026-07-02T10:00:00Z"
    # 120 ug/m3 PM2.5 is deep in the Unhealthy band -> sub-index >= 151
    assert pm25["aqi_sub_index"] >= 151
    o3 = next(r for r in records if r["pollutant"] == "o3")
    assert o3["aqi_sub_index"] < 51  # 0.03 ppm is Good
    assert all(r["ingested_at"] for r in records)


def test_poll_once_survives_a_failing_station():
    good = dict(LOCATION, id=1)
    bad = dict(LOCATION, id=2)
    client = FakeClient(
        {
            1: [{"sensorsId": 1, "value": 10.0, "datetime": {"utc": "2026-07-02T10:00:00Z"}}],
            2: RuntimeError("API blew up"),
        }
    )

    records = poll_once(client, [good, bad])

    # The failing station is logged and skipped; the good one still lands.
    assert len(records) == 1
    assert records[0]["location_id"] == 1
