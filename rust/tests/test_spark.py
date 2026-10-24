"""End-to-end tests of the Spark data source on a local Spark 4.1 session."""

import os
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from grib_idx.spark import GribIdxDataSource  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[2] / "src" / "test" / "resources"
GFS = str(FIXTURES / "gfs_sample.grib2")
HEIGHT_ABOVE_GROUND = "Specified height level above ground"

pytestmark = pytest.mark.skipif(not Path(GFS).exists(), reason="run download_test_fixtures.sh")


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.python.filterPushdown.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.dataSource.register(GribIdxDataSource)
    yield session
    session.stop()


def test_gridded_read_matches_the_scala_schema_and_selects_by_option(spark) -> None:
    df = spark.read.format("grib_idx").option("outputMode", "gridded").option("variables", "TMP").load(GFS)

    rows = df.where((F.col("level_type") == HEIGHT_ABOVE_GROUND) & (F.col("level_value") == 2.0)).collect()

    assert [f.name for f in df.schema][:10] == [
        "file_path", "message_index", "discipline", "variable_short_name", "variable_name",
        "units", "level_type", "level_value", "reference_time", "forecast_time",
    ]
    assert len(rows) == 1
    assert (rows[0].nx, rows[0].ny, len(rows[0]["values"])) == (360, 181, 360 * 181)


def test_flat_mode_is_the_default(spark) -> None:
    df = spark.read.format("grib_idx").option("variables", "PRMSL").option("longitudeRange", "-180to180").load(GFS)

    assert df.count() == 360 * 181
    assert df.agg(F.min("longitude"), F.max("longitude")).first() == (-179.0, 180.0)


def test_partitions_split_a_file_by_bytes(spark) -> None:
    df = spark.read.format("grib_idx").option("outputMode", "gridded").option("maxPartitionBytes", "4m").load(GFS)

    assert df.rdd.getNumPartitions() > 1
    assert df.count() == 696


def test_messages_per_partition(spark) -> None:
    df = spark.read.format("grib_idx").option("outputMode", "gridded").option("messagesPerPartition", "100").load(GFS)

    assert df.rdd.getNumPartitions() == 7


def test_pushed_filters_prune_and_are_evaluated_exactly(spark) -> None:
    df = (
        spark.read.format("grib_idx")
        .option("outputMode", "gridded")
        .option("filterPushdown", "true")
        .load(GFS)
        .where((F.col("variable_short_name") == "TMP") & (F.col("level_value") > 90000.0) & (F.col("units") == "K"))
    )

    rows = df.select("variable_short_name", "level_value").collect()

    assert rows and all(r.variable_short_name == "TMP" and r.level_value > 90000.0 for r in rows)
    assert df.rdd.getNumPartitions() == 1


def test_directory_and_level_type_option(spark) -> None:
    df = (
        spark.read.format("grib_idx")
        .option("outputMode", "gridded")
        .option("variables", "PRMSL,PMTF")
        .option("levelType", "mean sea")
        .load(str(FIXTURES))
    )

    assert {r.variable_short_name for r in df.select("variable_short_name").collect()} == {"PRMSL"}


# The data source's planning and reading run in Spark's Python workers, which
# coverage can't see, so they're also exercised directly here.


def test_reader_plans_byte_packed_partitions_and_reads_them_in_process() -> None:
    from grib_idx.planning import GribOptions
    from grib_idx.spark import GribIdxReader

    reader = GribIdxReader(GribOptions.parse({"path": GFS, "outputMode": "gridded", "parallelism": "4"}))

    partitions = reader.partitions()
    rows = sum(b.num_rows for p in partitions for b in reader.read(p))

    assert len(partitions) > 1
    assert all(p.messages for p in partitions)
    assert rows == 696


def test_push_filters_keeps_what_the_reader_cannot_evaluate() -> None:
    from pyspark.sql.datasource import EqualTo, GreaterThan, In, Not, StringStartsWith

    from grib_idx.planning import GribOptions
    from grib_idx.spark import GribIdxPushdownReader

    reader = GribIdxPushdownReader(GribOptions.parse({"path": GFS, "outputMode": "gridded"}))
    units = EqualTo(("units",), "K")
    prefix = StringStartsWith(("variable_short_name",), "T")
    filters = [EqualTo(("variable_short_name",), "TMP"), GreaterThan(("level_value",), 90000.0), units, prefix,
               Not(In(("level_type",), ["Mean sea level"]))]

    remaining = list(reader.pushFilters(filters))
    rows = [r for p in reader.partitions() for b in reader.read(p) for r in b.select(["variable_short_name", "level_value"]).to_pylist()]

    assert remaining == [units, prefix]
    assert reader.predicate[0] == "and"
    assert rows and all(r["variable_short_name"] == "TMP" and r["level_value"] > 90000.0 for r in rows)


def test_spark_schema_mirrors_the_arrow_schema() -> None:
    from grib_idx.spark import spark_schema

    ddl = spark_schema("flat")

    assert ddl.startswith("`file_path` STRING, `message_index` INT")
    assert "`value` DOUBLE" in ddl and "`product` STRUCT<`aerosol`: STRUCT<`type`: INT" in ddl
