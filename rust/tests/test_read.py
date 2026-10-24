"""Tests for the Python `read` API. Build first: `maturin develop --release`."""

from pathlib import Path

import pyarrow as pa
import pytest

import grib_idx

FIXTURES = Path(__file__).resolve().parents[2] / "src" / "test" / "resources"
GFS = FIXTURES / "gfs_sample.grib2"
TEMPLATES = FIXTURES / "templates_sample.grib2"

requires_fixtures = pytest.mark.skipif(not GFS.exists(), reason="run download_test_fixtures.sh")


@requires_fixtures
def test_read_returns_one_row_per_selected_field_in_the_gridded_schema() -> None:
    table = grib_idx.read(str(GFS), variables=["TMP"], levels=["2 m above ground", "500 mb"])

    assert table.schema == grib_idx.schema("gridded")
    assert table.column("variable_short_name").to_pylist() == ["TMP", "TMP"]
    assert sorted(table.column("level_type").to_pylist()) == ["Isobaric surface", "Specified height level above ground"]
    assert all(len(grid) == 360 * 181 for grid in table.column("values"))


@requires_fixtures
def test_flat_mode_has_one_row_per_point_with_signed_longitudes() -> None:
    table = grib_idx.read(str(GFS), variables=["PRMSL"], output_mode="flat", longitude_range="-180to180")

    assert table.num_rows == 360 * 181
    longitudes = table.column("longitude").to_pylist()
    assert min(longitudes) == -179.0 and max(longitudes) == 180.0


@requires_fixtures
def test_read_with_stats_reports_bytes_fetched_per_file() -> None:
    _, stats = grib_idx.read_with_stats(str(GFS), variables=["PRMSL"])

    assert len(stats) == 1
    assert stats[0].messages_read == 1
    assert stats[0].messages_in_index == 696
    assert 0 < stats[0].bytes_read < stats[0].file_size


@requires_fixtures
def test_predicate_filters_exactly() -> None:
    predicate = ("and", ("eq", "variable_short_name", "TMP"), ("gt", "level_value", 90000.0))

    table = grib_idx.read(str(GFS), predicate=predicate)

    assert table.num_rows > 0
    assert set(table.column("variable_short_name").to_pylist()) == {"TMP"}
    assert all(v > 90000.0 for v in table.column("level_value").to_pylist())


@requires_fixtures
def test_file_without_index_is_read_whole_with_product_attributes() -> None:
    table = grib_idx.read(str(TEMPLATES))

    products = table.column("product").to_pylist()
    assert table.num_rows == 7
    assert any(p["ensemble"] for p in products) and any(p["probability"] for p in products)


@requires_fixtures
def test_glob_reads_every_matching_file() -> None:
    table = grib_idx.read(str(FIXTURES / "g*_sample.grib2"), variables=["PRMSL", "PMTF"])

    assert {Path(p).name for p in table.column("file_path").to_pylist()} == {"gfs_sample.grib2", "gefs_chem_sample.grib2"}


@requires_fixtures
def test_selection_matching_nothing_returns_an_empty_table_with_the_schema() -> None:
    table = grib_idx.read(str(GFS), variables=["NO_SUCH_VARIABLE"])

    assert table.num_rows == 0
    assert table.schema == grib_idx.schema("gridded")


def test_unsupported_predicate_is_rejected() -> None:
    assert grib_idx.is_supported_predicate(("eq", "level_value", 500.0))
    assert not grib_idx.is_supported_predicate(("eq", "units", "K"))
    assert not grib_idx.is_supported_predicate(("gt", "variable_short_name", "A"))


def test_missing_local_file_raises_value_error() -> None:
    with pytest.raises(ValueError, match="no such file or directory"):
        grib_idx.read("does/not/exist.grib2")


def test_invalid_output_mode_raises_value_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outputMode"):
        grib_idx.schema("wide")


def test_non_grib_file_raises(tmp_path: Path) -> None:
    junk = tmp_path / "junk.grib2"
    junk.write_bytes(b"not a grib file")

    with pytest.raises(OSError, match="decode"):
        grib_idx.read(str(junk))


def test_returns_pyarrow_types() -> None:
    assert isinstance(grib_idx.schema("flat"), pa.Schema)


@requires_fixtures
def test_index_is_found_for_file_names_that_need_percent_encoding(tmp_path: Path) -> None:
    source = FIXTURES / "gefs_chem_sample.grib2"
    named = tmp_path / "gefs chem 100%.grib2"
    named.write_bytes(source.read_bytes())
    (tmp_path / "gefs chem 100%.grib2.idx").write_bytes(Path(f"{source}.idx").read_bytes())

    _, stats = grib_idx.read_with_stats(str(named), variables=["PMTF"])

    assert stats[0].messages_in_index == 15
    assert stats[0].bytes_read < stats[0].file_size


@requires_fixtures
def test_message_ranges_outside_the_file_are_rejected() -> None:
    size = GFS.stat().st_size

    with pytest.raises(ValueError, match="not within"):
        grib_idx.read_plan(str(GFS), size, [(0, size - 10, 2**63, [("TMP", "500 mb")])])
