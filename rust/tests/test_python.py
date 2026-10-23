"""Tests for the Python bindings. Build first: `maturin develop --release`."""

from pathlib import Path

import pyarrow as pa
import pytest

import grib_idx

FIXTURES = Path(__file__).resolve().parents[2] / "src" / "test" / "resources"
GFS = FIXTURES / "gfs_sample.grib2"

requires_gfs = pytest.mark.skipif(not GFS.exists(), reason="run download_test_fixtures.sh")


@requires_gfs
def test_read_returns_record_batch_with_one_row_per_selected_field() -> None:
    batch = grib_idx.read(str(GFS), variables=["TMP"], levels=["2 m above ground", "500 mb"])

    assert isinstance(batch, pa.RecordBatch)
    assert batch.column("variable_short_name").to_pylist() == ["TMP", "TMP"]
    assert sorted(batch.column("level_description").to_pylist()) == ["2 m above ground", "500 mb"]
    assert all(len(grid) == 360 * 181 for grid in batch.column("values"))


@requires_gfs
def test_read_with_stats_reports_bytes_fetched() -> None:
    _, stats = grib_idx.read_with_stats(str(GFS), variables=["PRMSL"])

    assert stats["messages_read"] == 1
    assert stats["messages_in_index"] == 696
    assert 0 < stats["bytes_read"] < stats["file_size"]


@requires_gfs
def test_selection_matching_nothing_returns_empty_batch() -> None:
    batch = grib_idx.read(str(GFS), variables=["NO_SUCH_VARIABLE"])

    assert batch.num_rows == 0
    assert "values" in batch.schema.names


def test_missing_local_file_raises_value_error() -> None:
    with pytest.raises(ValueError, match="invalid location"):
        grib_idx.read("does/not/exist.grib2")


def test_missing_index_sidecar_raises_io_error(tmp_path: Path) -> None:
    unindexed = tmp_path / "no_index.grib2"
    unindexed.write_bytes(b"GRIB")

    with pytest.raises(OSError, match="object store error"):
        grib_idx.read(str(unindexed))
