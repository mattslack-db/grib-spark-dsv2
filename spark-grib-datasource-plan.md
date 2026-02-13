# Spark DataSource V2 GRIB File Reader — Implementation Plan

## 1. Project Overview

Build a read-only Apache Spark 3.x DataSource V2 connector that allows users to read GRIB1 and GRIB2 meteorological files directly into Spark DataFrames. The target usage:

```scala
val df = spark.read
  .format("grib")                          // or "com.example.spark.grib"
  .option("variables", "TMP,RH")           // optional: filter variables
  .option("levelType", "isobaricInhPa")    // optional: filter level type
  .load("s3a://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f000")

df.printSchema()
// root
//  |-- variable: string
//  |-- name: string
//  |-- units: string
//  |-- level_type: string
//  |-- level_value: double
//  |-- forecast_time: timestamp
//  |-- reference_time: timestamp
//  |-- latitude: double
//  |-- longitude: double
//  |-- value: double
```

---

## 2. GRIB Format Background

GRIB (GRIdded Binary) is the WMO standard for distributing gridded meteorological data. Key characteristics that affect the design:

- A GRIB file is an **unordered collection of self-contained 2D records** (messages). Each message contains a single (x, y) grid of values for one variable at one level and one time step.
- Messages are concatenated together — there is no file-level header or index.
- Each message has its own header sections describing the grid geometry, parameter, level, and time.
- **GRIB1** and **GRIB2** are structurally different editions. GRIB2 supports more complex metadata, templates, and data compression (JPEG2000, PNG, complex packing).
- Files can be very large (hundreds of MB to several GB for a single GFS run), but each individual message is typically 50KB–5MB.
- An optional `.idx` sidecar index file (plain text) lists the byte offsets of each message — this is critical for efficient partial reads from S3.

---

## 3. Architecture & Key Design Decisions

### 3.1 GRIB Parsing Library: netCDF-Java (Unidata CDM)

Use the **netCDF-Java** library (`edu.ucar:cdm-core`) as the GRIB decoding engine. This is the most mature and comprehensive JVM-native GRIB reader available.

**Rationale:**
- Pure Java — no native dependencies (unlike ecCodes which requires a C library).
- Supports both GRIB1 and GRIB2.
- Handles WMO parameter tables plus many centre-specific local tables.
- Supports all GRIB grid types (lat/lon, Gaussian, Lambert conformal, polar stereographic, etc.).
- Provides low-level `Grib2RecordScanner` / `Grib1RecordScanner` for streaming record-by-record access, avoiding the need to materialise the entire dataset in memory.
- Well-maintained by Unidata / UCAR with active releases.

**Maven coordinates:**
```xml
<dependency>
  <groupId>edu.ucar</groupId>
  <artifactId>grib</artifactId>
  <version>5.6.0</version>  <!-- check for latest -->
</dependency>
<dependency>
  <groupId>edu.ucar</groupId>
  <artifactId>cdm-core</artifactId>
  <version>5.6.0</version>
</dependency>
```

### 3.2 Output Schema Strategy

**Flattened / long-format schema** (one row per grid point per message):

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | `StringType` | Source file URI |
| `message_index` | `IntegerType` | Ordinal position of the GRIB message in the file |
| `discipline` | `IntegerType` | GRIB2 discipline code (0=Meteorological, 10=Oceanographic, etc.) |
| `variable_short_name` | `StringType` | Short parameter name (e.g. `TMP`, `RH`, `UGRD`) |
| `variable_name` | `StringType` | Human-readable name (e.g. "Temperature") |
| `units` | `StringType` | Parameter units (e.g. "K", "%", "m/s") |
| `level_type` | `StringType` | Level type description (e.g. "isobaricInhPa", "surface", "heightAboveGround") |
| `level_value` | `DoubleType` | Level value (e.g. 500.0 for 500 hPa) |
| `reference_time` | `TimestampType` | Analysis / model initialisation time |
| `forecast_time` | `TimestampType` | Valid time of the forecast |
| `latitude` | `DoubleType` | Grid point latitude (degrees north) |
| `longitude` | `DoubleType` | Grid point longitude (degrees east, 0–360 or -180–180 based on option) |
| `value` | `DoubleType` | The data value at this grid point |

**Alternative: wide-format / gridded schema** (one row per message, data as array):

| Column | Type | Description |
|--------|------|-------------|
| `variable_short_name` | `StringType` | Parameter name |
| `level_type` | `StringType` | Level type |
| `level_value` | `DoubleType` | Level value |
| `reference_time` | `TimestampType` | Analysis time |
| `forecast_time` | `TimestampType` | Valid time |
| `nx` | `IntegerType` | Number of points in x dimension |
| `ny` | `IntegerType` | Number of points in y dimension |
| `lat_start` | `DoubleType` | First grid latitude |
| `lon_start` | `DoubleType` | First grid longitude |
| `lat_increment` | `DoubleType` | Grid spacing in latitude |
| `lon_increment` | `DoubleType` | Grid spacing in longitude |
| `values` | `ArrayType(DoubleType)` | Flattened grid data (row-major, ny × nx) |

**Recommendation:** Implement the long-format as the default (most flexible for SQL queries, joins, aggregations), and offer the gridded format via `.option("outputMode", "gridded")` for users who want to preserve the array structure for downstream processing.

### 3.3 Partitioning Strategy

Each GRIB **message** (or group of messages in the same file) maps to one Spark `InputPartition`. The partitioning tiers:

1. **File-level partitioning** — each input file becomes one or more partitions.
2. **Message-level partitioning** — if an `.idx` index file is available, individual messages (or groups of N messages) within a file can be assigned to separate partitions, enabling sub-file parallelism via HTTP range reads from S3.
3. **User-configurable** — `option("messagesPerPartition", "50")` controls granularity.

For S3 reads, leverage the `.idx` file byte offsets to issue targeted range requests rather than downloading entire multi-GB files.

### 3.4 S3 Access

- Use Hadoop's `s3a://` filesystem abstraction via `FileSystem.get()` — this gives free credential handling, retry logic, and S3 range reads.
- The `RandomAccessFile` in netCDF-Java won't work directly with S3. Instead, download the relevant byte ranges to local temp files (or use an in-memory buffer) before passing to the GRIB scanner.
- For the index-based approach: fetch `.idx` file first (small, typically < 1MB), parse offsets, then use `s3a` range reads for individual messages.

---

## 4. DataSource V2 API Implementation

### 4.1 Class Hierarchy

The Spark 3.x DataSource V2 read path requires implementing these interfaces:

```
DefaultSource (TableProvider, DataSourceRegister)
  └─ GribTable (Table, SupportsRead)
       └─ GribScanBuilder (ScanBuilder, SupportsPushDownFilters)
            └─ GribScan (Scan, Batch)
                 ├─ planInputPartitions() → Array[GribInputPartition]
                 └─ createReaderFactory() → GribPartitionReaderFactory
                      └─ GribPartitionReader (PartitionReader[InternalRow])
```

### 4.2 Class Details

#### `DefaultSource` — Entry Point

```
package: com.example.spark.grib

extends: TableProvider, DataSourceRegister

responsibilities:
  - Registered short name: "grib"
  - inferSchema(): scans a sample file to determine schema
  - getTable(): returns a GribTable with parsed options

options parsed:
  - "path" / paths (standard Spark)
  - "variables" — comma-separated filter list
  - "levelType" — filter by level type string
  - "outputMode" — "flat" (default) or "gridded"
  - "messagesPerPartition" — int, default 20
  - "longitudeRange" — "0to360" (default) or "-180to180"
  - "useIndex" — boolean, default true (look for .idx sidecar)
```

Registration via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` pointing to `com.example.spark.grib.DefaultSource`.

#### `GribTable`

```
extends: Table, SupportsRead

methods:
  name()         → "grib"
  schema()       → StructType (flat or gridded based on outputMode)
  capabilities() → Set(BATCH_READ)
  newScanBuilder(options) → GribScanBuilder
```

#### `GribScanBuilder`

```
extends: ScanBuilder, SupportsPushDownFilters

responsibilities:
  - Accept pushed-down filters on variable_short_name, level_type,
    level_value, reference_time, forecast_time
  - Store pushed filters for use during partition planning and reading
  - build() → GribScan

filter push-down:
  - EqualTo("variable_short_name", "TMP")  → pushed
  - In("variable_short_name", ["TMP","RH"]) → pushed
  - EqualTo("level_type", "isobaricInhPa")  → pushed
  - GreaterThan("level_value", 500)          → pushed
  - Filters on lat/lon/value → NOT pushed (post-scan only)
```

#### `GribScan` / `GribBatch`

```
extends: Scan, Batch

planInputPartitions():
  1. List all input file paths (expand globs)
  2. For each file, check for .idx sidecar
  3. If .idx exists and useIndex=true:
       - Parse .idx to get message offsets + metadata
       - Apply pushed variable/level filters to exclude messages
       - Group remaining messages into partitions of messagesPerPartition
       - Each GribInputPartition stores: filePath, List[(byteOffset, byteLength)]
  4. If no .idx:
       - One GribInputPartition per file (whole-file read)
  5. Return Array[GribInputPartition]

createReaderFactory() → GribPartitionReaderFactory

readSchema() → StructType
```

#### `GribInputPartition`

```
extends: InputPartition, Serializable

fields:
  - filePath: String
  - messageRanges: Option[Seq[(Long, Long)]]  // (offset, length) pairs
  - pushedFilters: Array[Filter]               // serialized for executor
```

#### `GribPartitionReaderFactory`

```
extends: PartitionReaderFactory

createReader(partition: InputPartition) → GribPartitionReader
```

#### `GribPartitionReader`

```
extends: PartitionReader[InternalRow]

lifecycle:
  1. Constructor: open file (local/S3), initialise GRIB scanner
  2. next(): advance to next row
     - If outputMode=flat: next grid point (within current message,
       or advance to next message)
     - If outputMode=gridded: next message
  3. get(): return current InternalRow
  4. close(): release resources, delete temp files

implementation detail for flat mode:
  - Maintain state: currentMessage, currentPointIndex
  - For each message: decode grid, iterate all (lat, lon, value) triples
  - Apply any non-pushed filters (lat/lon bounds etc.)
  - Construct InternalRow using UTF8String for strings, doubles, timestamps

implementation detail for S3 range reads:
  - If messageRanges is defined:
      for each (offset, length) range:
        issue s3a range read → byte[]
        wrap in ByteArrayInputStream
        feed to Grib2RecordScanner or Grib1RecordScanner
  - If messageRanges is None:
      stream entire file through scanner
```

---

## 5. Index (.idx) File Parsing

GFS/HRRR/NBM `.idx` files follow this format:

```
1:0:d=2024010100:HGT:1 mb:anl:
2:83345:d=2024010100:TMP:1 mb:anl:
3:152498:d=2024010100:RH:1 mb:anl:
```

Fields: `messageNumber:byteOffset:d=YYYYMMDDHH:variable:level:forecastInfo:`

**Parser design:**

```scala
case class GribIndexEntry(
  messageNumber: Int,
  byteOffset: Long,
  referenceTime: String,    // "2024010100"
  variableName: String,     // "TMP"
  levelDescription: String, // "1 mb", "surface", "2 m above ground"
  forecastInfo: String      // "anl", "6 hour fcst"
)

object GribIndexParser {
  def parse(idxContent: String): Seq[GribIndexEntry]
  def computeMessageLengths(entries: Seq[GribIndexEntry], fileSize: Long): Seq[(Long, Long)]
}
```

Message lengths are inferred from consecutive byte offsets (offset[n+1] - offset[n]), with the last message extending to EOF.

---

## 6. Build Configuration

### 6.1 SBT (recommended)

```scala
// build.sbt
name := "spark-grib"
version := "0.1.0"
scalaVersion := "2.12.18"  // match Spark 3.x

val sparkVersion = "3.5.3"
val netcdfVersion = "5.6.0"

libraryDependencies ++= Seq(
  // Spark (provided — user's cluster supplies these)
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",

  // GRIB parsing
  "edu.ucar" % "grib"     % netcdfVersion,
  "edu.ucar" % "cdm-core" % netcdfVersion,
  "edu.ucar" % "bufr"     % netcdfVersion,  // transitive dep

  // S3 support (provided — typically on cluster)
  "org.apache.hadoop" % "hadoop-aws"   % "3.3.6" % "provided",
  "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262" % "provided",

  // Testing
  "org.scalatest"     %% "scalatest"   % "3.2.18"  % "test",
  "org.apache.spark"  %% "spark-sql"   % sparkVersion % "test",
  "org.apache.spark"  %% "spark-core"  % sparkVersion % "test",
  "org.mockito"       %% "mockito-scala-scalatest" % "1.17.30" % "test"
)

// Unidata repository for netCDF-Java
resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/"

// Build fat JAR excluding Spark/Hadoop
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
```

### 6.2 Project Structure

```
spark-grib/
├── build.sbt
├── project/
│   ├── build.properties          # sbt.version=1.9.7
│   └── plugins.sbt               # addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
├── src/
│   ├── main/
│   │   ├── scala/com/example/spark/grib/
│   │   │   ├── DefaultSource.scala
│   │   │   ├── GribTable.scala
│   │   │   ├── GribScanBuilder.scala
│   │   │   ├── GribScan.scala
│   │   │   ├── GribInputPartition.scala
│   │   │   ├── GribPartitionReaderFactory.scala
│   │   │   ├── GribPartitionReader.scala
│   │   │   ├── GribIndexParser.scala
│   │   │   ├── GribMessageDecoder.scala      # wraps netCDF-Java decoding
│   │   │   ├── GribSchemas.scala             # StructType definitions
│   │   │   └── GribOptions.scala             # option parsing & defaults
│   │   └── resources/
│   │       └── META-INF/services/
│   │           └── org.apache.spark.sql.sources.DataSourceRegister
│   └── test/
│       ├── scala/com/example/spark/grib/
│       │   ├── unit/
│       │   │   ├── GribIndexParserSpec.scala
│       │   │   ├── GribMessageDecoderSpec.scala
│       │   │   ├── GribOptionsSpec.scala
│       │   │   ├── GribScanBuilderSpec.scala
│       │   │   └── GribSchemaSpec.scala
│       │   ├── integration/
│       │   │   ├── LocalFileReadSpec.scala
│       │   │   ├── S3ReadSpec.scala
│       │   │   ├── FilterPushDownSpec.scala
│       │   │   ├── MultiFileGlobSpec.scala
│       │   │   └── GriddedOutputSpec.scala
│       │   └── SparkTestBase.scala           # shared SparkSession for tests
│       └── resources/
│           ├── gfs_sample.grib2              # small test fixture (download script)
│           ├── hrrr_sample.grib2
│           ├── gfs_sample.grib2.idx
│           └── grib1_sample.grb
```

---

## 7. Testing Strategy

### 7.1 Test Fixtures — Local Files

Download small GRIB files for local test resources. Script to create fixtures:

```bash
#!/bin/bash
# download_test_fixtures.sh
# Downloads small GRIB2 files for unit/integration tests

mkdir -p src/test/resources

# GFS 0.25° — single forecast hour (f000 = analysis), ~30MB
# Filter to a few variables with wgrib2 to make it smaller, or use the 1° product (~5MB)
curl -o src/test/resources/gfs_sample.grib2 \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000"

# Matching index file
curl -o src/test/resources/gfs_sample.grib2.idx \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000.idx"

# HRRR — single hour, CONUS 3km (~100MB, large but good for perf tests)
# Use the sub-hourly wrfsubhf product which is smaller
curl -o src/test/resources/hrrr_sample.grib2 \
  "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20240101/conus/hrrr.t00z.wrfsubhf00.grib2"

# NBM — smaller file, good variety of variables
curl -o src/test/resources/nbm_sample.grib2 \
  "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20240101/00/core/blend.t00z.core.f001.co.grib2"

echo "Done. Test fixtures downloaded."
```

For CI, either commit small extracted subsets (< 5MB) or use the download script in a setup step.

### 7.2 Public S3 Test Buckets for Integration Testing

All of these are **publicly accessible with `--no-sign-request`** (no AWS credentials needed):

| Dataset | S3 Bucket | Example Path | Format | Typical Size | Notes |
|---------|-----------|-------------|--------|-------------|-------|
| **GFS 0.25°** | `s3://noaa-gfs-bdp-pds` | `gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f000` | GRIB2 | ~30MB | Has `.idx` sidecar. Best for integration tests. |
| **GFS 1.0°** | `s3://noaa-gfs-bdp-pds` | `gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000` | GRIB2 | ~5MB | Smaller, good for fast CI. Has `.idx`. |
| **HRRR** | `s3://noaa-hrrr-bdp-pds` | `hrrr.20240101/conus/hrrr.t00z.wrfprsf00.grib2` | GRIB2 | ~100MB | 3km CONUS. Has `.idx`. |
| **GFS Wave** | `s3://noaa-gfs-bdp-pds` | `gfs.20240101/00/wave/station/gfswave.t00z.atlocn.0p16.f000.grib2` | GRIB2 | ~1MB | Very small, good for quick tests. |
| **NBM** | `s3://noaa-nbm-grib2-pds` | `blend.20240101/00/core/blend.t00z.core.f001.co.grib2` | GRIB2 | ~10MB | National Blend of Models. Has `.idx`. |
| **GEFS** | `s3://noaa-gefs-pds` | `gefs.20240101/00/atmos/pgrb2ap5/gep01.t00z.pgrb2a.0p50.f006` | GRIB2 | ~15MB | Ensemble member files. |

**Spark S3 configuration for anonymous access in tests:**

```scala
spark.sparkContext.hadoopConfiguration.set(
  "fs.s3a.aws.credentials.provider",
  "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.region", "us-east-1")
```

### 7.3 Unit Tests

| Test Class | What It Tests |
|------------|---------------|
| `GribIndexParserSpec` | Parsing `.idx` files: correct offsets, variable names, level descriptions, edge cases (empty file, malformed lines, GRIB1-style index) |
| `GribMessageDecoderSpec` | Decoding a single GRIB2 message from bytes: correct variable name, level, grid dimensions, data values against known reference values |
| `GribOptionsSpec` | Option parsing: defaults, variable filter parsing, invalid option handling, case insensitivity |
| `GribScanBuilderSpec` | Filter push-down: verifying which filters are accepted vs rejected, correct pushed/post-scan filter partitioning |
| `GribSchemaSpec` | Schema construction: flat vs gridded mode, correct types, column ordering |

### 7.4 Integration Tests

| Test Class | What It Tests |
|------------|---------------|
| `LocalFileReadSpec` | End-to-end read of a local GRIB2 file via `spark.read.format("grib").load(path)`. Verify row count, schema, sample values against known reference data. |
| `S3ReadSpec` | Read from public S3 bucket (`noaa-gfs-bdp-pds`). Verify connectivity, correct data, index-based partitioning. Tag as `@SlowTest` for CI gating. |
| `FilterPushDownSpec` | Apply variable/level filters via `.where()`, verify that pushed filters reduce partition count and row count. Compare filtered vs unfiltered results. |
| `MultiFileGlobSpec` | Read multiple files via glob pattern. Verify correct union of data, file_path column populated correctly. |
| `GriddedOutputSpec` | Read with `outputMode=gridded`. Verify array dimensions match nx × ny, values array length is correct. |
| `Grib1ReadSpec` | Read a GRIB1 format file. Verify compatibility and correct decoding. |
| `LargeFilePerformanceSpec` | Benchmark read of a full GFS 0.25° file. Measure time, partition count, memory. Tag as `@PerformanceTest`. |

### 7.5 Shared Test Infrastructure

```scala
// SparkTestBase.scala
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("GribDataSourceTest")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  // Helper: load a test resource as a file path
  def testResource(name: String): String =
    getClass.getClassLoader.getResource(name).getPath

  // Helper: configure S3 anonymous access
  def configureAnonymousS3(): Unit = {
    spark.sparkContext.hadoopConfiguration.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
  }
}
```

### 7.6 Test Tags and CI Configuration

```scala
// Tag annotations
object SlowTest extends Tag("com.example.tags.SlowTest")
object PerformanceTest extends Tag("com.example.tags.PerformanceTest")

// Usage
test("read from S3", SlowTest) { ... }
```

**SBT test configuration:**
```scala
// Fast unit tests only (CI on every commit)
Test / testOptions += Tests.Argument("-l", "com.example.tags.SlowTest")

// Full suite including S3 (nightly CI)
// sbt "testOnly * -- -n com.example.tags.SlowTest"
```

---

## 8. Implementation Phases

### Phase 1: Core Reader (Weeks 1–2)

**Goal:** Read a single local GRIB2 file into a DataFrame with flat schema.

| Task | Details |
|------|---------|
| Project scaffolding | SBT build, dependency resolution, project structure |
| `GribSchemas` | Define StructType for flat and gridded output modes |
| `GribOptions` | Parse and validate all reader options |
| `GribMessageDecoder` | Wrap netCDF-Java: open file → scan messages → decode each message's metadata + data grid → yield (metadata, lat[], lon[], values[]) |
| `DefaultSource` + `GribTable` | TableProvider entry point, schema inference by reading first file |
| `GribScanBuilder` | Basic builder (no filter push-down yet) |
| `GribScan` | File-level partitioning only (one partition per file) |
| `GribPartitionReaderFactory` + `GribPartitionReader` | Full flat-mode reader: iterate messages, iterate grid points, emit InternalRow |
| `META-INF/services` registration | Wire up DataSourceRegister |
| Unit + integration tests | `GribMessageDecoderSpec`, `LocalFileReadSpec` |

**Exit criteria:** `spark.read.format("grib").load("local/file.grib2").show()` works.

### Phase 2: Index-Based Partitioning + Filter Push-Down (Weeks 3–4)

**Goal:** Sub-file partitioning using `.idx` files, filter push-down for variable/level.

| Task | Details |
|------|---------|
| `GribIndexParser` | Parse `.idx` sidecar files, compute message byte ranges |
| `GribScan` update | If `.idx` exists, plan partitions at message granularity |
| `GribInputPartition` | Carry byte offset ranges for targeted reads |
| `GribPartitionReader` update | Support reading from byte range (local file seek or S3 range) |
| `GribScanBuilder` update | Implement `SupportsPushDownFilters` — accept variable, level, time filters |
| Filter integration | Use pushed filters during partition planning (skip messages in `.idx`) and during reading (skip messages during scan) |
| Tests | `GribIndexParserSpec`, `FilterPushDownSpec` |

**Exit criteria:** Filtering by variable reduces the number of partitions and rows. Reading a single variable from a 300-message file only decodes the relevant messages.

### Phase 3: S3 Support (Week 5)

**Goal:** Seamless reading from `s3a://` paths with range-read optimisation.

| Task | Details |
|------|---------|
| S3 file access | Use Hadoop `FileSystem` API to read files and `.idx` sidecars from S3 |
| Range read support | For index-based partitions, fetch only the required byte ranges from S3 |
| Temp file management | Buffer range-read bytes to temporary local files or `ByteArrayInputStream` for the GRIB scanner |
| Anonymous credential support | Test and document anonymous access for NOAA public buckets |
| Tests | `S3ReadSpec` against `noaa-gfs-bdp-pds` |

**Exit criteria:** `spark.read.format("grib").load("s3a://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000").count()` returns correct row count.

### Phase 4: Gridded Output Mode + GRIB1 (Week 6)

**Goal:** Support `outputMode=gridded` and GRIB1 format files.

| Task | Details |
|------|---------|
| Gridded schema + reader | Emit one row per message with `values` as `Array[Double]` |
| GRIB1 support | Add `Grib1RecordScanner` path in `GribMessageDecoder`, handle GRIB1 table lookups |
| Longitude normalisation | Implement `-180to180` option for longitude remapping |
| Multi-file glob | Support `load("path/to/*.grib2")` patterns |
| Tests | `GriddedOutputSpec`, `Grib1ReadSpec`, `MultiFileGlobSpec` |

### Phase 5: Performance + Hardening (Week 7–8)

**Goal:** Production readiness.

| Task | Details |
|------|---------|
| Performance benchmarking | Profile against full GFS 0.25° files, identify bottlenecks |
| Memory optimisation | Use `UnsafeRow` / columnar batch reader if beneficial, minimise object allocation in hot path |
| Error handling | Graceful handling of corrupt messages, missing `.idx`, unsupported grid types, network timeouts |
| Column pruning | Implement `SupportsPushDownRequiredColumns` — if user only selects metadata columns, skip data decoding entirely |
| Documentation | README, Scaladoc, usage examples |
| Fat JAR + publish | Assembly JAR for `--packages` or `--jars` deployment |
| CI pipeline | GitHub Actions with unit tests on every PR, nightly S3 integration tests |

---

## 9. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **netCDF-Java JAR size** | The full CDM + GRIB JARs are ~50MB, creating a large assembly JAR | Shade/relocate to avoid conflicts. Consider using only the `grib` module with minimal transitive deps. Profile what's actually needed. |
| **netCDF-Java thread safety** | GRIB scanners may not be thread-safe | Each `PartitionReader` creates its own scanner instance — no shared state across partitions. |
| **S3 latency for many small range reads** | Sub-file partitioning could create thousands of small HTTP requests | Batch adjacent messages into single range reads. Set a minimum partition size (e.g. 1MB). Use `messagesPerPartition` to control. |
| **GRIB table mismatches** | netCDF-Java may not have tables for all centres/local parameters | Log warnings for unknown parameters, emit raw code numbers as fallback. Allow user-supplied table overrides via option. |
| **Large grid expansion (flat mode)** | A single 0.25° global message has 1,038,240 grid points — flat mode creates >1M rows per message | Document this trade-off. Recommend gridded mode for global data. Offer spatial bounding box filter to limit expansion. |
| **Dependency conflicts with user's Spark cluster** | netCDF-Java's transitive deps (Guava, protobuf, etc.) may clash | Use `sbt-assembly` shading for all non-Spark dependencies. |

---

## 10. Future Enhancements

- **Write support** — convert DataFrames back to GRIB2 (via netCDF-Java or ecCodes JNI).
- **Spatial predicate push-down** — bounding box filters on lat/lon pushed to skip messages whose grid doesn't intersect.
- **Streaming support** — watch an S3 prefix for new GRIB files (e.g. real-time forecast ingest).
- **Columnar batch reader** — implement `supportColumnarReads()` for vectorised execution.
- **Catalog integration** — register as a Spark catalog for `SELECT * FROM grib.\`s3a://bucket/path\``.
- **Delta/Iceberg sink** — built-in pipeline to convert GRIB → Delta Lake tables with partitioning by variable/level/time.
