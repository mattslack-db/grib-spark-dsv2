# Databricks notebook source
# MAGIC %md
# MAGIC # Reading NOAA GFS GRIB2 files from a Unity Catalog volume
# MAGIC
# MAGIC This notebook downloads [NOAA Global Forecast System (GFS)](https://registry.opendata.aws/noaa-gfs-bdp-pds/)
# MAGIC GRIB2 files into a **Unity Catalog volume** and reads them with the `grib` Spark DataSource V2 reader
# MAGIC from this repo. Section 9 does the same with the most recent **GEFS-Aerosols** run, copied directly from
# MAGIC [NOMADS](https://nomads.ncep.noaa.gov/).
# MAGIC
# MAGIC **Compute requirements**
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---|---|
# MAGIC | Compute type | Classic all-purpose or job cluster (serverless can't load custom Scala data sources) |
# MAGIC | Databricks Runtime | **16.4 LTS** (Spark 3.5, **Scala 2.12**). DBR 17+ is Spark 4 / Scala 2.13 and is not compatible with this build |
# MAGIC | Access mode | **Dedicated** (single user) |
# MAGIC | Library | `spark-grib-assembly-0.1.0.jar` installed as a cluster library, e.g. from `/Volumes/<catalog>/<schema>/<volume>/jars/` |
# MAGIC
# MAGIC See `examples/databricks/README.md` for the CLI commands that build the JAR, upload it to the volume,
# MAGIC and create the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "grib_demo", "Schema")
dbutils.widgets.text("volume", "noaa", "Volume")
dbutils.widgets.text("gfs_date", "20240101", "GFS run date (YYYYMMDD)")
dbutils.widgets.dropdown("gfs_cycle", "00", ["00", "06", "12", "18"], "GFS cycle (UTC)")
dbutils.widgets.dropdown("resolution", "1p00", ["1p00", "0p50", "0p25"], "Grid resolution")
dbutils.widgets.text("forecast_hours", "0,3,6,9,12", "Forecast hours (comma-separated)")
dbutils.widgets.text("nomads_forecast_hours", "0,3", "NOMADS GEFS-Aerosols forecast hours")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
GFS_DATE = dbutils.widgets.get("gfs_date")
GFS_CYCLE = dbutils.widgets.get("gfs_cycle")
RESOLUTION = dbutils.widgets.get("resolution")
FORECAST_HOURS = [int(h) for h in dbutils.widgets.get("forecast_hours").split(",") if h.strip()]
NOMADS_FORECAST_HOURS = [int(h) for h in dbutils.widgets.get("nomads_forecast_hours").split(",") if h.strip()]

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RUN_DIR = f"{VOLUME_ROOT}/gfs/{GFS_DATE}/{GFS_CYCLE}/{RESOLUTION}"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.gfs_{RESOLUTION}_surface"

print(f"GRIB files -> {RUN_DIR}")
print(f"Delta table -> {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the schema and volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
dbutils.fs.mkdirs(RUN_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Download GFS files from NOAA into the volume
# MAGIC
# MAGIC NOAA publishes GFS on the AWS Open Data registry (`noaa-gfs-bdp-pds`), which is reachable over plain
# MAGIC HTTPS from any cloud. We download each forecast hour's GRIB2 file **and its `.idx` sidecar**. The reader
# MAGIC uses the `.idx` file to split one GRIB file into many Spark partitions and to skip unwanted variables
# MAGIC without decoding them.
# MAGIC
# MAGIC Files keep NOAA's names (e.g. `gfs.t00z.pgrb2.1p00.f000` and `gfs.t00z.pgrb2.1p00.f000.idx`). When you point
# MAGIC the reader at a directory it reads every file except sidecars, checksums and partial downloads.

# COMMAND ----------

import os
import shutil
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

NOAA_BASE_URL = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
DOWNLOAD_RETRIES = 3
DOWNLOAD_TIMEOUT_SECONDS = 300
DOWNLOAD_PARALLELISM = 4
COPY_BUFFER_BYTES = 8 * 1024 * 1024


def gfs_url(forecast_hour: int) -> str:
    name = f"gfs.t{GFS_CYCLE}z.pgrb2.{RESOLUTION}.f{forecast_hour:03d}"
    return f"{NOAA_BASE_URL}/gfs.{GFS_DATE}/{GFS_CYCLE}/atmos/{name}"


def download(url: str, dest: str) -> str:
    """Stream `url` to `dest`. Skips files that already exist, and retries transient failures."""
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return f"exists   {dest}"
    tmp = f"{dest}.part"
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_SECONDS) as resp, open(tmp, "wb") as out:
                shutil.copyfileobj(resp, out, COPY_BUFFER_BYTES)
            os.replace(tmp, dest)
            return f"fetched  {dest} ({os.path.getsize(dest) / 1e6:.1f} MB)"
        except Exception as e:  # noqa: BLE001 - retry any network/IO error, then re-raise
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt == DOWNLOAD_RETRIES:
                raise RuntimeError(f"Failed to download {url} after {attempt} attempts") from e
            time.sleep(2 ** attempt)


def download_forecast_hour(forecast_hour: int) -> list:
    url = gfs_url(forecast_hour)
    grib_dest = f"{RUN_DIR}/{url.rsplit('/', 1)[1]}"
    return [download(url, grib_dest), download(f"{url}.idx", f"{grib_dest}.idx")]


with ThreadPoolExecutor(max_workers=DOWNLOAD_PARALLELISM) as pool:
    for lines in pool.map(download_forecast_hour, FORECAST_HOURS):
        print("\n".join(lines))

# COMMAND ----------

display(dbutils.fs.ls(RUN_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check that the reader is installed

# COMMAND ----------

# Resolve "grib" exactly as spark.read.format("grib") does. (A plain Class.forName via py4j uses the
# JVM's default classloader, which can't see cluster libraries on Databricks.)
try:
    spark._jvm.org.apache.spark.sql.execution.datasources.DataSource.lookupDataSource(
        "grib", spark._jsparkSession.sessionState().conf()
    )
except Exception as e:
    raise RuntimeError(
        "The GRIB reader JAR is not on the cluster classpath. Install spark-grib-assembly-0.1.0.jar as a "
        "cluster library (see examples/databricks/README.md) and restart the cluster."
    ) from e
print("GRIB reader found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explore the files
# MAGIC
# MAGIC A GFS file contains several hundred GRIB messages (one per variable and level). The `.idx` sidecar lists them,
# MAGIC so it's a cheap way to see what's available before decoding anything.

# COMMAND ----------

first_idx = f"{RUN_DIR}/gfs.t{GFS_CYCLE}z.pgrb2.{RESOLUTION}.f{FORECAST_HOURS[0]:03d}.idx"
with open(first_idx) as f:
    idx_lines = f.read().splitlines()
print(f"{len(idx_lines)} messages per file, e.g.:")
print("\n".join(idx_lines[:10]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Read with the `grib` data source (flat mode)
# MAGIC
# MAGIC Flat mode emits one row per grid point per message. The `variables` option and any `variable_short_name`
# MAGIC filters are used with the `.idx` file to skip messages *before* they are decoded.

# COMMAND ----------

from pyspark.sql import functions as F

surface = (
    spark.read.format("grib")
    .option("variables", "TMP,RH,UGRD,VGRD,PRMSL")
    .option("longitudeRange", "-180to180")
    .load(RUN_DIR)
)

surface.printSchema()
print(f"{surface.rdd.getNumPartitions()} input partitions")

# COMMAND ----------

# Which levels exist for the variables we selected? The level_type values come from the GRIB2 code tables.
display(
    surface.groupBy("variable_short_name", "level_type")
    .agg(F.countDistinct("level_value").alias("levels"), F.min("level_value").alias("min_level"), F.max("level_value").alias("max_level"))
    .orderBy("variable_short_name", "level_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter push-down
# MAGIC
# MAGIC Filters on `variable_short_name`, `level_type`, `level_value`, `discipline`, `reference_time` and `forecast_time`
# MAGIC are pushed into the scan. In the physical plan below they show up under `PushedFilters`.

# COMMAND ----------

LEVEL_TYPE_HEIGHT_ABOVE_GROUND = "Specified height level above ground"

t2m = (
    surface.where(
        (F.col("variable_short_name") == "TMP")
        & (F.col("level_type") == LEVEL_TYPE_HEIGHT_ABOVE_GROUND)
        & (F.col("level_value") == 2.0)
    )
    .withColumn("temp_c", F.col("value") - 273.15)
)
t2m.explain()

# COMMAND ----------

# Forecast 2 m temperature for London, all forecast hours, at the nearest grid point
LONDON_LAT, LONDON_LON = 51.51, -0.13
GRID_STEP_DEGREES = float(RESOLUTION.replace("p", "."))  # "1p00" -> 1.0, "0p25" -> 0.25


def nearest_grid_point(degrees: float) -> float:
    return round(degrees / GRID_STEP_DEGREES) * GRID_STEP_DEGREES + 0.0  # "+ 0.0" turns -0.0 into 0.0


display(
    t2m.where(
        (F.col("latitude") == nearest_grid_point(LONDON_LAT))
        & (F.col("longitude") == nearest_grid_point(LONDON_LON))
    )
    .select("reference_time", "forecast_time", "latitude", "longitude", F.round("temp_c", 1).alias("temp_c"))
    .orderBy("forecast_time")
)

# COMMAND ----------

# Global min / mean / max 2 m temperature per forecast hour
display(
    t2m.groupBy("forecast_time")
    .agg(F.round(F.min("temp_c"), 1).alias("min_c"), F.round(F.avg("temp_c"), 1).alias("mean_c"), F.round(F.max("temp_c"), 1).alias("max_c"))
    .orderBy("forecast_time")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save to a Delta table
# MAGIC
# MAGIC Writing the decoded data to Delta means downstream SQL, dashboards and Genie don't need the custom JAR.

# COMMAND ----------

(
    surface.where(F.col("level_type").isin(LEVEL_TYPE_HEIGHT_ABOVE_GROUND, "Mean sea level"))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

display(spark.sql(f"""
    SELECT variable_short_name, level_type, level_value, units, count(*) AS rows
    FROM {TABLE_NAME}
    GROUP BY ALL
    ORDER BY ALL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gridded mode + plotting
# MAGIC
# MAGIC Gridded mode emits one row per message, with the full grid as an array. That's a better fit for
# MAGIC array/NumPy-style processing, such as plotting a map.

# COMMAND ----------

gridded = (
    spark.read.format("grib")
    .option("outputMode", "gridded")
    .option("variables", "TMP")
    .load(RUN_DIR)
    .where(
        (F.col("level_type") == LEVEL_TYPE_HEIGHT_ABOVE_GROUND)
        & (F.col("level_value") == 2.0)
    )
)

display(gridded.select("forecast_time", "nx", "ny", "lat_start", "lon_start", "lat_increment", "lon_increment", F.size("values").alias("n_values")))

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

row = gridded.orderBy("forecast_time").first()
grid = np.array(row["values"], dtype=float).reshape(row["ny"], row["nx"]) - 273.15
lats = row["lat_start"] + np.arange(row["ny"]) * row["lat_increment"]
lons = row["lon_start"] + np.arange(row["nx"]) * row["lon_increment"]

fig, ax = plt.subplots(figsize=(12, 6))
mesh = ax.pcolormesh(lons, lats, grid, cmap="RdYlBu_r", shading="auto")
fig.colorbar(mesh, ax=ax, label="2 m temperature (°C)")
ax.set_title(f"GFS {GFS_DATE} {GFS_CYCLE}Z, valid {row['forecast_time']}")
ax.set_xlabel("Longitude (°E)")
ax.set_ylabel("Latitude (°N)")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Recent data from NOMADS: GEFS-Aerosols
# MAGIC
# MAGIC The AWS Open Data bucket is good for history. For the latest runs you can copy straight from
# MAGIC [NOMADS](https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/), which keeps only about the last 10 days.
# MAGIC Here we take the 00Z run of the GEFS-Aerosols model (`chem/pgrb2ap5`, 0.5° grid). Each file holds 960 messages:
# MAGIC fine (`PMTF`) and coarse (`PMTC`) particulate matter for 15 aerosol species and size bins, on 64 hybrid levels.
# MAGIC
# MAGIC NOMADS limits request rates, so keep the number of forecast hours small. Each file is about 200 MB.

# COMMAND ----------

import datetime
import urllib.error

NOMADS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod"
NOMADS_CYCLE = "00"
NOMADS_LOOKBACK_DAYS = 10


def gefs_chem_name(forecast_hour: int) -> str:
    return f"gefs.chem.t{NOMADS_CYCLE}z.a3d_0p50.f{forecast_hour:03d}.grib2"


def gefs_chem_dir_url(run_date: str) -> str:
    return f"{NOMADS_BASE_URL}/gefs.{run_date}/{NOMADS_CYCLE}/chem/pgrb2ap5"


def url_exists(url: str) -> bool:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def latest_complete_gefs_chem_run() -> str:
    """Newest run date whose last requested forecast hour (and its .idx) has been published."""
    today = datetime.datetime.now(datetime.timezone.utc).date()
    last_hour = max(NOMADS_FORECAST_HOURS)
    for days_back in range(NOMADS_LOOKBACK_DAYS):
        run_date = (today - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
        if url_exists(f"{gefs_chem_dir_url(run_date)}/{gefs_chem_name(last_hour)}.idx"):
            return run_date
    raise RuntimeError(f"No complete GEFS-Aerosols {NOMADS_CYCLE}Z run found on NOMADS in the last {NOMADS_LOOKBACK_DAYS} days")


GEFS_RUN_DATE = latest_complete_gefs_chem_run()
GEFS_DIR = f"{VOLUME_ROOT}/gefs_chem/{GEFS_RUN_DATE}/{NOMADS_CYCLE}/pgrb2ap5"
os.makedirs(GEFS_DIR, exist_ok=True)
print(f"Latest complete run: {GEFS_RUN_DATE} {NOMADS_CYCLE}Z -> {GEFS_DIR}")

# Sequential on purpose, to stay well inside NOMADS' rate limits.
for hour in NOMADS_FORECAST_HOURS:
    url = f"{gefs_chem_dir_url(GEFS_RUN_DATE)}/{gefs_chem_name(hour)}"
    dest = f"{GEFS_DIR}/{gefs_chem_name(hour)}"
    print(download(url, dest))
    print(download(f"{url}.idx", f"{dest}.idx"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aerosol species
# MAGIC
# MAGIC The 15 `PMTF`/`PMTC` grids at each level differ only by aerosol species and size bin. GEFS-Aerosols uses GRIB2
# MAGIC product template 4.48, so the reader puts these in the `product.aerosol` struct: `type_name`, plus the size
# MAGIC interval in metres (`size_interval_type` follows GRIB2 code table 4.91).

# COMMAND ----------

# Lowest model level (hybrid level 1) is closest to the surface.
LOWEST_HYBRID_LEVEL = 1.0
MICROMETRES_PER_METRE = 1e6

aerosols = (
    spark.read.format("grib")
    .option("variables", "PMTF,PMTC")
    .load(GEFS_DIR)
    .where(F.col("level_value") == LOWEST_HYBRID_LEVEL)
)

display(
    aerosols.groupBy(
        "forecast_time",
        "variable_short_name",
        F.col("product.aerosol.type_name").alias("aerosol"),
        F.round(F.col("product.aerosol.size_first_m") * MICROMETRES_PER_METRE, 3).alias("size_first_um"),
        F.round(F.col("product.aerosol.size_second_m") * MICROMETRES_PER_METRE, 3).alias("size_second_um"),
        "units",
    )
    .agg(F.round(F.avg("value"), 3).alias("global_mean"), F.round(F.max("value"), 1).alias("max"))
    .orderBy("forecast_time", "variable_short_name", F.desc("global_mean"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total particulate matter near the surface
# MAGIC
# MAGIC Summing every species and size bin at hybrid level 1 gives total particulate matter. Filtering on
# MAGIC `product.aerosol.type_name` picks out a single species, in this case dust. Values are in µg m⁻³.

# COMMAND ----------

first_file = f"{GEFS_DIR}/{gefs_chem_name(NOMADS_FORECAST_HOURS[0])}"
grids = (
    spark.read.format("grib")
    .option("outputMode", "gridded")
    .option("variables", "PMTF,PMTC")
    .load(first_file)
    .where(F.col("level_value") == LOWEST_HYBRID_LEVEL)
    .select("product.aerosol.type_name", "units", "forecast_time", "nx", "ny",
            "lat_start", "lon_start", "lat_increment", "lon_increment", "values")
    .collect()
)

ref = grids[0]
lats = ref["lat_start"] + np.arange(ref["ny"]) * ref["lat_increment"]
lons = ref["lon_start"] + np.arange(ref["nx"]) * ref["lon_increment"]


def summed(rows: list) -> np.ndarray:
    return sum(np.array(r["values"], dtype=float) for r in rows).reshape(ref["ny"], ref["nx"])


panels = [
    (f"Total PM ({len(grids)} species/size bins)", summed(grids)),
    ("Dust (all size bins)", summed([g for g in grids if g["type_name"] == "Dust Dry"])),
]

fig, axes = plt.subplots(2, 1, figsize=(12, 11))
for ax, (title, grid) in zip(axes, panels):
    mesh = ax.pcolormesh(lons, lats, np.log10(np.maximum(grid, 1e-3)), cmap="YlOrBr", shading="auto")
    fig.colorbar(mesh, ax=ax, label=f"log10 ({ref['units']})")
    ax.set_title(f"{title}, hybrid level 1")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
fig.suptitle(f"GEFS-Aerosols {GEFS_RUN_DATE} {NOMADS_CYCLE}Z, valid {ref['forecast_time']}")
fig.tight_layout()
plt.show()
