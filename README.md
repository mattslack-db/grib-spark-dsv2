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

df.where("variable_short_name = 'TMP' AND level_value = 500")
  .select("latitude", "longitude", "value")
  .show()
```

## Features

- **GRIB1 & GRIB2** support via netCDF-Java (pure Java, no native dependencies)
- **Two output modes:**
  - `flat` (default) — one row per grid point per message, ideal for SQL queries
  - `gridded` — one row per message with values as `Array[Double]`, preserving grid structure
- **Index-based partitioning** — uses `.idx` sidecar files for sub-file parallelism and efficient S3 range reads
- **Filter push-down** on `variable_short_name`, `level_type`, `level_value`, `discipline`, `reference_time`, `forecast_time`
- **S3 support** — reads from `s3a://` paths via Hadoop filesystem with anonymous access for NOAA public buckets
- **Glob patterns** — `spark.read.format("grib").load("path/to/*.grib2")`

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `variables` | *(all)* | Comma-separated variable filter (e.g. `"TMP,RH,UGRD"`) |
| `levelType` | *(all)* | Filter by level type string |
| `outputMode` | `flat` | `"flat"` or `"gridded"` |
| `messagesPerPartition` | `20` | Messages per Spark partition (index-based mode) |
| `longitudeRange` | `0to360` | `"0to360"` or `"-180to180"` |
| `useIndex` | `true` | Look for `.idx` sidecar files for sub-file partitioning |

## Building

Requires SBT and JDK 8+.

```bash
# Compile
sbt compile

# Run unit tests
sbt test

# Run all tests including integration (requires test fixtures)
./download_test_fixtures.sh
sbt "testOnly * -- -n com.example.spark.grib.tags.SlowTest"

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
