# spark-grib — Spark DataSource V2 GRIB File Reader

A read-only Apache Spark 3.x DataSource V2 connector for reading GRIB1 and GRIB2 meteorological files directly into Spark DataFrames.

## Quick Start

```scala
val df = spark.read
  .format("grib")
  .option("variables", "TMP,RH")           // optional: filter variables
  .option("levelType", "isobaricInhPa")    // optional: filter level type
  .load("s3a://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f000")

df.printSchema()
// root
//  |-- file_path: string
//  |-- message_index: integer
//  |-- discipline: integer
//  |-- variable_short_name: string
//  |-- variable_name: string
//  |-- units: string
//  |-- level_type: string
//  |-- level_value: double
//  |-- reference_time: timestamp
//  |-- forecast_time: timestamp
//  |-- latitude: double
//  |-- longitude: double
//  |-- value: double
//  |-- pds_template: integer
//  |-- level_value2: double
//  |-- product: struct
//  |    |-- aerosol: struct
//  |    |    |-- type: integer
//  |    |    |-- type_name: string
//  |    |    |-- size_interval_type: integer
//  |    |    |-- size_first_m: double
//  |    |    |-- ... (size_second_m, wavelength_*)
//  |    |-- ensemble: struct (perturbation_type, perturbation_type_name, member, ensemble_size)
//  |    |-- ensemble_derived: struct (type, type_name, ensemble_size)
//  |    |-- probability: struct (type, type_name, lower_limit, upper_limit)
//  |    |-- percentile: integer
//  |    |-- statistic: struct (process, process_name, interval_hours)

df.where("variable_short_name = 'TMP' AND level_value = 500")
  .select("latitude", "longitude", "value")
  .show()
```

## Features

- **GRIB1 & GRIB2** support via netCDF-Java (pure Java, no native dependencies)
- **Two output modes:**
  - `flat` (default) — one row per grid point per message, ideal for SQL queries
  - `gridded` — one row per message with values as `Array[Double]`, preserving grid structure
- **Index-based partitioning** — uses `.idx` sidecar files to split files into byte-sized partitions spread across the cluster, each fetched with one sequential read per run of adjacent messages
- **Filter push-down** on `variable_short_name`, `level_type`, `level_value` and `discipline`. With an `.idx` sidecar, messages that can't match are skipped before they're read or decoded: by variable always, and by level for the GRIB2 level forms in NCEP `.idx` files: pressure, height and depth levels and layers (`500 mb`, `2 m above ground`, `0-0.1 m below ground`, `30-0 mb above ground`), hybrid and sigma levels, PV surfaces, and named levels such as `surface`, `tropopause` and `max wind`. Levels it doesn't recognise are never pruned
- **S3 support** — reads from `s3a://` paths via Hadoop filesystem with anonymous access for NOAA public buckets
- **Directories and glob patterns** — `spark.read.format("grib").load("path/to/gfs.t00z.pgrb2.1p00.f*")`. Every file is read as GRIB except `.idx` sidecars, checksums, netCDF-Java index files, hidden files and partial downloads, so NOAA's extension-less names work as-is

## Product-definition columns

In GRIB2, what identifies a field can go beyond variable, level and time, depending on the message's product
definition template. Three nullable columns, appended to both output modes, carry this:

| Column | Description |
|--------|-------------|
| `pds_template` | GRIB2 product definition template number (e.g. `0` analysis/forecast, `48` aerosol). Null for GRIB1 |
| `level_value2` | Second bound of a layer (e.g. `1.0` for "0.33-1 sigma layer"). Null for single levels and GRIB1 |
| `product.aerosol` | Templates 4.46/4.48: aerosol `type` (code table 4.233) and `type_name`, plus size and wavelength intervals in metres, as GRIB2 encodes them (`*_interval_type` from code table 4.91, then the first and second limits) |
| `product.ensemble` | Templates 4.1/4.11: an ensemble member's `perturbation_type` (code table 4.6), `member` number and `ensemble_size` |
| `product.ensemble_derived` | Templates 4.2/4.12: a product derived from all members, e.g. `type_name` "Unweighted mean of all members" (code table 4.7) |
| `product.probability` | Templates 4.5/4.9: probability `type` (code table 4.9) and its `lower_limit`/`upper_limit`, in the parameter's units |
| `product.percentile` | Templates 4.6/4.10: the percentile, e.g. `1` for the 1% level |
| `product.statistic` | Templates 4.8-4.12: statistical `process` over a time interval (code table 4.10, e.g. "Accumulation") and `interval_hours`. `forecast_time` is the end of the interval |

Each `product` field is null unless the message's template carries it, and a template can carry several: an
ensemble member's 6-hour accumulation (4.11) has both `ensemble` and `statistic`.

```scala
spark.read.format("grib").load("gefs.chem.t00z.a3d_0p50.f003.grib2")
  .where("product.aerosol.type_name = 'Dust Dry' AND level_value = 1")

spark.read.format("grib").load("blend.t00z.qmd.f024.co.grib2")
  .where("variable_short_name = 'APCP' AND product.percentile = 90")
```

Units come from netCDF-Java's parameter tables. Where those are known to be wrong they're corrected: NCEP's
aerosol parameters (`PMTC`, `PMTF`, `LPMTF`, `LIPMF`) are reported in µg m-3, as NCEP documents them, not g m-3.

Filters on these columns are applied by Spark after decoding; they aren't pushed down.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `variables` | *(all)* | Comma-separated variable filter (e.g. `"TMP,RH,UGRD"`), case-insensitive. A field also matches by its `.idx` name where that differs from the decoded one (e.g. `CLMR` for `CLWMR`) |
| `levelType` | *(all)* | Filter by level type string |
| `outputMode` | `flat` | `"flat"` or `"gridded"` |
| `maxPartitionBytes` | `134217728` (128 MiB) | Upper bound on bytes per partition (index-based mode). Partitions are sized to spread the data that survives filtering across the cluster's cores, like Spark's file sources |
| `messagesPerPartition` | *(unset)* | Fixed number of messages per partition instead of byte-based sizing (index-based mode) |
| `longitudeRange` | `0to360` | `"0to360"` or `"-180to180"` |
| `useIndex` | `true` | Look for `.idx` sidecar files for sub-file partitioning |

## Building

Requires SBT and JDK 8, 11 or 17 (Spark 3.5 doesn't support newer JDKs; CI uses 17).

```bash
# Compile
sbt compile

# Run unit tests
sbt test

# Run all tests, including integration tests on real NOAA data
./download_test_fixtures.sh
sbt testAll

# Build fat JAR
sbt assembly
```

The assembly JAR will be at `target/scala-2.12/spark-grib-assembly-0.1.0.jar`.

## Usage on a Spark Cluster

```bash
spark-shell --jars spark-grib-assembly-0.1.0.jar
```

For S3 anonymous access (NOAA public data):

```scala
spark.sparkContext.hadoopConfiguration.set(
  "fs.s3a.aws.credentials.provider",
  "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

val df = spark.read.format("grib")
  .option("variables", "TMP")
  .load("s3a://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000")
```

## Databricks Example

[`examples/databricks/`](examples/databricks/) contains a Python notebook that downloads NOAA GFS files into a Unity Catalog volume and reads them with this connector. It also has CLI setup steps for the JAR and cluster.

## Project Structure

```
src/main/scala/com/example/spark/grib/
├── DefaultSource.scala           # TableProvider entry point ("grib" format)
├── GribTable.scala               # Table + SupportsRead
├── GribScanBuilder.scala         # ScanBuilder + SupportsPushDownFilters
├── GribScan.scala                # Scan + Batch — partition planning
├── GribInputPartition.scala      # Serializable partition descriptor
├── GribPartitionReaderFactory.scala
├── GribPartitionReader.scala     # Core read loop (flat + gridded)
├── GribMessageDecoder.scala      # netCDF-Java GRIB1/GRIB2 wrapper
├── GribIndexParser.scala         # .idx sidecar file parser
├── GribOptions.scala             # Option parsing & validation
└── GribSchemas.scala             # StructType definitions
```

## GRIB Parsing

Uses [netCDF-Java](https://www.unidata.ucar.edu/software/netcdf-java/) (Unidata CDM) `edu.ucar:grib:5.6.0` for decoding. This is pure Java with no native dependencies, supports both GRIB editions, all standard grid types, and WMO + centre-specific parameter tables.

## Known Limitations

- **NBM present-weather fields decode as mostly missing.** netCDF-Java mis-decodes some GRIB2 messages that use complex packing with missing-value management (data representation template 5.2 or 5.3). In the NBM test fixture (`blend.t00z.core.f001.co.grib2` from `download_test_fixtures.sh`), message 227 (`PWTHER`, present weather) comes back with 3,682,099 of its 3,744,965 points as NaN and a maximum of 80. ecCodes decodes no missing points and a maximum of 86. Every netCDF-Java release from 5.6.0 to 5.11.0 (the latest) gives the same wrong result, so upgrading doesn't help. No workaround is in place, and no upstream issue was found. Treat these fields from this connector as unreliable.
- **`.idx` pruning knows only two name aliases.** wgrib2's `.idx` short names usually match netCDF-Java's decoded names, ignoring case. The known exceptions are `CLMR`/`CLWMR` and `LFTX`/`LFT X`, and they are handled. Another pair that differs beyond case would be pruned by an index read that filters on the decoded name. If a filter returns no rows that you expect, compare it with `useIndex=false`, and add the pair to `IdxNameAliases` in `GribFilters.scala`.
