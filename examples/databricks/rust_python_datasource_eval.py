# Databricks notebook source
# MAGIC %md
# MAGIC # Rust `grib_idx` reader vs the Scala `grib` reader on a UC volume
# MAGIC
# MAGIC Compares the Rust reader (`rust/`, a cluster wheel library, used through its PySpark Python data source
# MAGIC `grib_idx.spark.GribIdxDataSource`) with the Scala DSv2 reader (`spark-grib-assembly` JAR) on the same GFS
# MAGIC files:
# MAGIC
# MAGIC 1. Direct `grib_idx.read` calls on the driver: the volume's FUSE path and NOAA's public S3 bucket
# MAGIC 2. Timings of selective and full reads, gridded and flat, through both readers
# MAGIC 3. Parity of the two readers' gridded output: every column, and every value element by element
# MAGIC 4. Parity of flat output: per-file aggregates of every point
# MAGIC
# MAGIC Results are returned as JSON through `dbutils.notebook.exit`.
# MAGIC
# MAGIC **Requirements:** the `grib-demo` cluster from `README.md` (DBR 16.4 LTS, dedicated mode) with both the JAR and
# MAGIC the wheel (`rust/scripts/build_linux_wheel.sh`) installed as cluster libraries.

# COMMAND ----------

dbutils.widgets.text("run_dir", "/Volumes/main/grib_demo/noaa/gfs/20240101/00/1p00", "GFS run directory")
dbutils.widgets.text("s3_url", "s3://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f006", "Public S3 file")
dbutils.widgets.text("repeats", "2", "Timing repeats (best kept)")

RUN_DIR = dbutils.widgets.get("run_dir")
S3_URL = dbutils.widgets.get("s3_url")
REPEATS = int(dbutils.widgets.get("repeats"))

NOAA_S3_OPTIONS = {"skip_signature": "true", "region": "us-east-1"}
HEIGHT_ABOVE_GROUND = "Specified height level above ground"
RESULTS = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Direct reads on the driver

# COMMAND ----------

import dataclasses
import time

import grib_idx


def timed_read(path, **kwargs):
    start = time.perf_counter()
    table, stats = grib_idx.read_with_stats(path, **kwargs)
    totals = {k: sum(getattr(s, k) for s in stats) for k in ("file_size", "messages_read", "bytes_read")}
    return {"seconds": round(time.perf_counter() - start, 3), "rows": table.num_rows, "files": len(stats), **totals}


RESULTS["driver"] = {
    "volume_selective": timed_read(RUN_DIR, variables=["TMP"], levels=["2 m above ground"]),
    "volume_full": timed_read(RUN_DIR),
    "s3_selective": timed_read(
        S3_URL,
        variables=["TMP", "UGRD", "VGRD"],
        levels=["2 m above ground", "10 m above ground"],
        storage_options=NOAA_S3_OPTIONS,
    ),
}
print(RESULTS["driver"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Timings through Spark

# COMMAND ----------

from pyspark.sql import functions as F

from grib_idx.spark import GribIdxDataSource

spark.dataSource.register(GribIdxDataSource)
PARALLELISM = str(spark.sparkContext.defaultParallelism)


def rust_df(mode, selective):
    reader = spark.read.format("grib_idx").option("outputMode", mode).option("parallelism", PARALLELISM)
    if selective:
        reader = reader.option("variables", "TMP").option("levels", "2 m above ground")
    return reader.load(RUN_DIR)


def scala_df(mode, selective):
    reader = spark.read.format("grib").option("outputMode", mode)
    if not selective:
        return reader.load(RUN_DIR)
    return (
        reader.option("variables", "TMP")
        .load(RUN_DIR)
        .where((F.col("level_type") == HEIGHT_ABOVE_GROUND) & (F.col("level_value") == 2.0))
    )


def best_seconds(make_df):
    times = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        make_df().write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - start)
    return round(min(times), 2)


RESULTS["timings_seconds"] = {
    f"{reader}_{mode}_{'selective' if selective else 'full'}": best_seconds(lambda: make(mode, selective))
    for mode in ("gridded", "flat")
    for selective in (True, False)
    for reader, make in (("rust", rust_df), ("scala", scala_df))
}
print(RESULTS["timings_seconds"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gridded parity: every column, every value
# MAGIC
# MAGIC Joins the two readers' full gridded output on (file name, message index). Values are compared element by
# MAGIC element (NaN matching NaN); the relative tolerance allows two float32 ULPs.

# COMMAND ----------

VALUE_TOLERANCE = 2.4e-7
GEOMETRY_TOLERANCE = 1e-5
EXACT_COLUMNS = ["discipline", "variable_short_name", "variable_name", "units", "level_type", "reference_time",
                 "forecast_time", "pds_template", "nx", "ny"]


def keyed(df, alias):
    return df.withColumn("file", F.regexp_extract("file_path", r"([^/]+)$", 1)).alias(alias)


def nan_equal(a, b):
    return (F.isnan(a) & F.isnan(b)) | (a == b) | (a.isNull() & b.isNull())


def rel_close(a, b, tolerance):
    return nan_equal(a, b) | (F.abs(a - b) <= F.lit(tolerance) * F.greatest(F.abs(b), F.lit(1.0)))


worst_value = F.aggregate(
    F.zip_with(
        "r.values",
        "s.values",
        lambda a, b: F.when(F.isnan(a) & F.isnan(b), 0.0)
        .when(F.isnan(a) | F.isnan(b), 1e30)
        .otherwise(F.abs(a - b) / F.greatest(F.abs(b), F.lit(1e-30))),
    ),
    F.lit(0.0),
    lambda acc, x: F.greatest(acc, x),
)

joined = keyed(rust_df("gridded", False), "r").join(keyed(scala_df("gridded", False), "s"), ["file", "message_index"], "full_outer")
mismatch_counts = {
    c: F.count(F.when(~(F.col(f"r.{c}").eqNullSafe(F.col(f"s.{c}"))), 1)) for c in EXACT_COLUMNS
} | {
    c: F.count(F.when(~rel_close(F.col(f"r.{c}"), F.col(f"s.{c}"), GEOMETRY_TOLERANCE), 1))
    for c in ("level_value", "lat_start", "lon_start", "lat_increment", "lon_increment")
} | {
    "level_value2": F.count(F.when(~rel_close(F.col("r.level_value2"), F.col("s.level_value2"), GEOMETRY_TOLERANCE), 1)),
    "product": F.count(F.when(F.to_json("r.product") != F.to_json("s.product"), 1)),
    "values": F.count(F.when(worst_value > VALUE_TOLERANCE, 1)),
}
parity = joined.agg(
    F.count(F.lit(1)).alias("messages"),
    F.count(F.when(F.col("r.nx").isNull(), 1)).alias("only_scala"),
    F.count(F.when(F.col("s.nx").isNull(), 1)).alias("only_rust"),
    F.max(worst_value).alias("max_relative_value_diff"),
    *[expr.alias(f"mismatched_{c}") for c, expr in mismatch_counts.items()],
).first().asDict()
RESULTS["gridded_parity"] = parity
print(parity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Flat parity: per-file aggregates over every point

# COMMAND ----------


def flat_summary(df):
    return (
        keyed(df, "x")
        .groupBy("file")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.isnan("value"), 0.0).otherwise(F.col("value"))).alias("value_sum"),
            F.sum(F.isnan("value").cast("long")).alias("missing"),
            F.sum("latitude").alias("lat_sum"),
            F.sum("longitude").alias("lon_sum"),
        )
    )


flat = flat_summary(rust_df("flat", True)).alias("r").join(flat_summary(scala_df("flat", True)).alias("s"), "file", "full_outer")
flat_rows = flat.select(
    "file",
    F.col("r.rows").alias("rust_rows"),
    F.col("s.rows").alias("scala_rows"),
    (F.col("r.missing") == F.col("s.missing")).alias("missing_equal"),
    (F.abs(F.col("r.value_sum") - F.col("s.value_sum")) / F.abs(F.col("s.value_sum"))).alias("value_sum_rel_diff"),
    (F.abs(F.col("r.lat_sum") - F.col("s.lat_sum"))).alias("lat_sum_diff"),
    (F.abs(F.col("r.lon_sum") - F.col("s.lon_sum"))).alias("lon_sum_diff"),
).collect()
RESULTS["flat_parity"] = [r.asDict() for r in flat_rows]
print(RESULTS["flat_parity"])

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(RESULTS, default=str))
