import json

import pytest

from ingestion.sinks import LocalFileSink, get_sink


def test_local_file_sink_writes_jsonl(tmp_path):
    sink = LocalFileSink(tmp_path / "raw")
    records = [{"city": "Dublin", "value": 8.2}, {"city": "Delhi", "value": 120.5}]

    written = sink.write(records)

    assert written == 2
    files = list((tmp_path / "raw").glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(l)["city"] for l in lines] == ["Dublin", "Delhi"]


def test_local_file_sink_empty_write_creates_no_file(tmp_path):
    sink = LocalFileSink(tmp_path / "raw")
    assert sink.write([]) == 0
    assert list((tmp_path / "raw").glob("*.jsonl")) == []


def test_each_write_creates_a_new_file(tmp_path):
    # One file per poll cycle is what lets Spark's file-source streaming
    # trigger see new data incrementally.
    sink = LocalFileSink(tmp_path / "raw")
    sink.write([{"a": 1}])
    sink.write([{"a": 2}])
    assert len(list((tmp_path / "raw").glob("*.jsonl"))) == 2


def test_get_sink_local(tmp_path):
    sink = get_sink("local", directory=tmp_path)
    assert isinstance(sink, LocalFileSink)


def test_get_sink_unknown_kind_raises():
    with pytest.raises(ValueError, match="Unknown sink kind"):
        get_sink("kafka", directory="/nowhere")
