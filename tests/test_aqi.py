from common.aqi import compute_aqi, sub_index, aqi_category, is_dangerous, DANGEROUS_AQI_THRESHOLD


def test_pm25_breakpoint_boundaries():
    # Reference points from the EPA AQI breakpoint table (effective 2024-05-06)
    assert sub_index("pm25", 9.0, "ug/m3") == 50
    assert sub_index("pm25", 9.1, "ug/m3") == 51
    assert sub_index("pm25", 35.4, "ug/m3") == 100
    assert sub_index("pm25", 55.4, "ug/m3") == 150
    assert sub_index("pm25", 55.5, "ug/m3") == 151
    assert sub_index("pm25", 125.4, "ug/m3") == 200


def test_pm25_interpolation_midpoint():
    # Midpoint of the Unhealthy band (55.5-125.4 -> 151-200) should land
    # roughly halfway between 151 and 200.
    mid_c = (55.5 + 125.4) / 2
    idx = sub_index("pm25", mid_c, "ug/m3")
    assert 174 <= idx <= 177


def test_unsupported_unit_returns_none():
    assert sub_index("pm25", 40, "mystery-unit") is None


def test_gas_ppb_conversion_for_no2():
    # NO2 breakpoints are defined directly in ppb.
    assert sub_index("no2", 53, "ppb") == 50
    assert sub_index("no2", 100, "ppb") == 100


def test_gas_ugm3_conversion_for_o3():
    # 0.070 ppm O3 ~= 137.2 ug/m3 (MW 48, 24.45 L/mol); should land at the
    # top of the Moderate band (AQI 100), not error out.
    idx_ppm = sub_index("o3", 0.070, "ppm")
    idx_ugm3 = sub_index("o3", 137.2, "ug/m3")
    assert idx_ppm == 100
    # EPA's 3-decimal ppm truncation is coarse relative to O3's narrow
    # 0.015 ppm-wide bands, so allow a few AQI points of slack here.
    assert abs(idx_ugm3 - idx_ppm) <= 5


def test_category_labels():
    assert aqi_category(25) == "Good"
    assert aqi_category(75) == "Moderate"
    assert aqi_category(125) == "Unhealthy for Sensitive Groups"
    assert aqi_category(175) == "Unhealthy"
    assert aqi_category(250) == "Very Unhealthy"
    assert aqi_category(400) == "Hazardous"


def test_dangerous_threshold():
    assert DANGEROUS_AQI_THRESHOLD == 151
    assert is_dangerous(150) is False
    assert is_dangerous(151) is True


def test_compute_aqi_picks_worst_pollutant():
    readings = {
        "pm25": (10.0, "ug/m3"),  # AQI ~51
        "o3": (0.090, "ppm"),  # AQI ~161 (Unhealthy) - should dominate
    }
    result = compute_aqi(readings)
    assert result is not None
    assert result.dominant_pollutant == "o3"
    assert result.aqi >= DANGEROUS_AQI_THRESHOLD
    assert result.dangerous is True
    assert result.category == "Unhealthy"


def test_compute_aqi_no_supported_pollutants():
    assert compute_aqi({"radon": (5, "pci/l")}) is None


def test_above_top_breakpoint_clamps_instead_of_dropping():
    # A reading far above the published PM2.5 table ceiling should still be
    # scored as hazardous rather than silently discarded.
    idx = sub_index("pm25", 900, "ug/m3")
    assert idx == 500
