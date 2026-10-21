# Databricks example: NOAA GFS in a Unity Catalog volume

[`noaa_gfs_uc_volume.py`](noaa_gfs_uc_volume.py) is a Databricks notebook (source format) that:

1. Creates a UC schema and volume
2. Downloads NOAA GFS GRIB2 files and their `.idx` sidecars from the AWS Open Data bucket into the volume over HTTPS (works from Azure, AWS or GCP)
3. Reads them with `spark.read.format("grib")` in flat and gridded modes, showing filter push-down
4. Saves the decoded data to a Delta table and plots a 2 m temperature map
5. Finds the newest GEFS-Aerosols run on [NOMADS](https://nomads.ncep.noaa.gov/), copies it into the volume,
   breaks particulate matter down by aerosol species using the `product.aerosol` columns, and maps total PM and
   dust near the surface

## Requirements

- **Classic compute.** Serverless can't load custom Scala data sources.
- **DBR 16.4 LTS (Scala 2.12).** The JAR targets Spark 3.5 / Scala 2.12. DBR 17+ is Spark 4 / Scala 2.13.
- **Dedicated (single user) access mode**
- `CREATE SCHEMA` / `CREATE VOLUME` on the target catalog (or an existing schema and volume)

## Setup with the Databricks CLI

Replace `<profile>`, `<catalog>`, `<schema>` and `<volume>`.

```bash
# 1. Build the JAR
sbt assembly

# 2. Create the schema + volume and upload the JAR
databricks schemas create <schema> <catalog> --profile <profile>
databricks volumes create <catalog> <schema> <volume> MANAGED --profile <profile>
databricks fs cp target/scala-2.12/spark-grib-assembly-0.1.0.jar \
  dbfs:/Volumes/<catalog>/<schema>/<volume>/jars/spark-grib-assembly-0.1.0.jar --profile <profile>

# 3. Create a dedicated-mode DBR 16.4 LTS cluster (Azure node type shown)
databricks clusters create --profile <profile> --json '{
  "cluster_name": "grib-demo",
  "spark_version": "16.4.x-scala2.12",
  "node_type_id": "Standard_D4ds_v5",
  "num_workers": 2,
  "data_security_mode": "SINGLE_USER",
  "single_user_name": "<your-user@company.com>",
  "autotermination_minutes": 30
}'

# 4. Install the JAR on the cluster
databricks libraries install --profile <profile> --json '{
  "cluster_id": "<cluster-id>",
  "libraries": [{"jar": "/Volumes/<catalog>/<schema>/<volume>/jars/spark-grib-assembly-0.1.0.jar"}]
}'

# 5. Import the notebook
databricks workspace import /Users/<your-user@company.com>/noaa_gfs_uc_volume \
  --file examples/databricks/noaa_gfs_uc_volume.py --language PYTHON --format SOURCE --profile <profile>
```

Then open the notebook, attach it to the cluster, set the widgets (catalog, schema, volume, GFS date and cycle,
resolution, forecast hours), and run all cells.

## Notes

- Files keep NOAA's original names. When you load a directory (or a glob), the reader skips `.idx` sidecars,
  checksums, hidden files and partial downloads (`.part`), and reads everything else as GRIB.
- Approximate file sizes per forecast hour: `1p00` ≈ 40 MB, `0p50` ≈ 150 MB, `0p25` ≈ 500 MB.
- NOMADS keeps only about the last 10 days of runs and limits request rates, so the notebook downloads its files
  one at a time and only the forecast hours in the `nomads_forecast_hours` widget (default `0,3`, ~200 MB each).
- Once data is in the Delta table, readers such as SQL warehouses, dashboards and Genie don't need the JAR.
