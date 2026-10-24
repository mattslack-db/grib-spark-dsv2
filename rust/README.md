# grib-idx: Rust GRIB2 reader for Python and Spark

A Rust reimplementation of this repo's Scala `grib` Spark data source, callable
from Python. It keeps the indexed lookup from object storage: it fetches the
`.idx` sidecar, prunes messages on the requested variables, levels and
filters, range-reads only their bytes (via
[`object_store`](https://crates.io/crates/object_store): S3, ADLS, GCS, HTTP,
local and `/Volumes` paths), decodes them in parallel with
[grib-rs](https://crates.io/crates/grib), and returns Arrow in **the same
flat and gridded schemas as the Scala reader**, named with the same
netCDF-Java tables.

```python
import grib_idx

table = grib_idx.read(
    "s3://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f006",
    variables=["TMP", "UGRD", "VGRD"],
    levels=["2 m above ground", "10 m above ground"],
    storage_options={"skip_signature": "true", "region": "us-east-1"},
)  # pyarrow.Table, gridded: one row per field
```

```python
from grib_idx.spark import GribIdxDataSource

spark.dataSource.register(GribIdxDataSource)
df = (spark.read.format("grib_idx")
      .option("outputMode", "gridded")
      .option("variables", "TMP")
      .load("/Volumes/catalog/schema/volume/gfs/"))
```

## Feature comparison with the Scala reader

| Feature | Scala `grib` | Rust `grib_idx` |
|---|---|---|
| Flat / gridded output, same columns and types | yes | yes (identical schemas) |
| Names, units, level types | netCDF-Java tables | the same tables, exported (`data/*.tsv`) |
| `product.*` (aerosol, ensemble, derived, probability, percentile, statistic) | yes | yes |
| Statistically processed fields' forecast time (interval end) | yes | yes |
| `variables`, `levelType`, `outputMode`, `longitudeRange`, `useIndex` | yes | yes |
| `messagesPerPartition`, `maxPartitionBytes`, byte-packed partitions within a file | yes | yes (port of `PartitionPlanning`) |
| Filter pushdown with idx pruning (`GribFilters`, `GribIndexLevels`) | yes | yes on Spark 4.1+ (`filterPushdown` option); Spark 3.5 has no Python pushdown, so use options |
| Files without `.idx` (read whole) | yes | yes, decoded in parallel per message |
| Directories, globs, sidecar/non-GRIB skipping | yes (Hadoop FS) | yes (`object_store` listing, same skip rules) |
| `dbfs:/Volumes/...` paths | yes | yes (read through `/Volumes`) |
| Regular lat/lon and Lambert grids, flat lat/lon | yes | yes |
| Other projected grids (polar stereographic, Mercator, rotated, Gaussian) | yes | flat lat/lon yes (grib-rs); gridded origin/increments NaN |
| Alternating-row scanning (e.g. NBM) | yes | yes (rows straightened as netCDF-Java does) |
| GRIB1 | yes | **no** (grib-rs is GRIB2 only): clear error |
| Non-NCEP local tables (ECMWF, etc.) | yes | WMO names only |
| Undecodable messages | skipped with a warning | skipped, reported as Python warnings |
| Use outside Spark (pandas, polars, DuckDB via Arrow) | no | yes |
| `.idx` level selection (`levels`) | no | yes (extra option) |

## Bugs found in the Scala reader

Both are reproducible with the fixtures and fixed (or avoided) in the Rust reader:

1. **`variables=CLMR` / `CLWMR` both return nothing.** Index pruning assumes the
   `.idx` short name equals the decoded one up to case, but for 24 GFS fields
   wgrib2 and netCDF-Java disagree (`CLMR`/`CLWMR`, `LFTX`/`LFT X`). The `.idx`
   keeps `CLMR` messages, then the exact decoded check (`CLWMR`) drops them;
   asking for `CLWMR` prunes them from the `.idx`. Rust matches either name.
2. **NBM present weather (`PWTHER`) is 98% NaN.** netCDF-Java 5.6 mis-decodes
   complex packing (template 5.2) with missing-value management: it marks
   3.68M of 3.74M points missing and the rest differ by up to 78. ecCodes
   (via pygrib) agrees with the Rust reader exactly.

## Decisions (made unattended; easy to revisit)

- **Schema parity over the spike's extras.** Output matches the Scala schemas
  exactly (`array<double>` values, the `product` struct). The Python `read()`
  defaults to gridded; the Spark data source defaults to flat, as Scala does.
- **Names from netCDF-Java, not grib-rs.** `ExportGribTables.scala` exports
  the WMO and NCEP tables netCDF-Java uses (with `ParameterUnits` and
  `AerosolTypeNames` corrections), embedded at build time. Other centres'
  local tables aren't exported.
- **`variables` matches the decoded or the `.idx` name** (case-insensitive),
  and SQL pruning knows the observed wgrib2 aliases, avoiding Scala bug 1.
- **netCDF-Java's quirks are reproduced** where they define the output: the
  interval length adds each range's time increment (NBM's 1 h accumulations
  with a 1 h increment report 2 h); on a unit mismatch, where netCDF-Java
  throws and Scala empties `product`, Rust leaves only `interval_hours` null.
- **Non-GRIB bytes are an error**, where netCDF-Java silently reads nothing
  (also catches `.idx` offsets that don't match their file).
- **A `.idx` whose offsets don't ascend within the file** (e.g. stale after
  the file was replaced) is ignored with a warning and the file read whole,
  like a malformed one; ranges passed in from Python are bounds-checked.
- **Name pruning is conservative:** `.idx` names that aren't plain
  abbreviations (wgrib2's `var discipline=0 ... parm=195` for unnamed
  parameters) and decoded fallback names (`var0_16_195`) never prune.
- **`levels` needs an `.idx`**: on a file without one it raises rather than
  silently matching nothing.
- **Pushdown is opt-in** (`filterPushdown=true`): Spark 4.1 raises if a
  reader implements `pushFilters` while
  `spark.sql.python.filterPushdown.enabled` is off.
- **Partition sizing uses a `parallelism` option** (default: the planning
  machine's CPUs), since a Python data source can't see the cluster; pass
  `spark.sparkContext.defaultParallelism`.
- **Object store options** are passed as `storage.<key>` data source options
  (there is no Hadoop configuration).
- **Pure-Rust codecs** (hayro JPEG2000, rust-aec, png), so the abi3 wheel
  builds without a C toolchain and runs on any Python >= 3.10.
- **Memory:** a partition's bytes are fetched once and decoded a chunk of
  messages (one per core) at a time, so decoded values aren't all held at once.

## Parity with the Scala reader

`scripts/parity.py` compares every gridded column, every value and the first
message's flat lat/lons with a netCDF-Java dump (`DumpDecodedMessages.scala`),
reading through the `.idx` and, with `--whole-file`, without it:

| Fixture | Messages | Result |
|---|---|---|
| GFS 1° analysis | 696 | pass (idx and whole file) |
| GFS 1° f006 (statistical intervals) | 743 | pass (idx and whole file) |
| GEFS-Aerosols (template 4.48) | 15 | pass (idx and whole file) |
| RAP (Lambert, sub-messages) | 19 | pass (idx and whole file) |
| Product templates 4.1/2/8/9/10/11/12 | 7 | pass |
| NBM (Lambert, alternating rows, no `.idx`) | 294 | pass, except netCDF-Java's defect above |

Values agree to 1 float32 ULP (max relative difference 1.19e-7); names,
units, levels, times and `product` exactly. Projected-grid geometry differs
by under 0.3 m and 2 m in lat/lon (independent projection implementations).

## Running

```bash
cd rust
cargo test --release                       # Rust unit + fixture tests (fixtures: ../download_test_fixtures.sh)
uv venv .venv && uv pip install --python .venv/bin/python maturin numpy pytest pytest-cov pyarrow "pyspark>=4.1"
.venv/bin/maturin develop --release        # build and install the package
.venv/bin/python -m pytest tests/          # Python API, planning and local Spark 4.1 tests
./scripts/build_linux_wheel.sh             # manylinux x86_64 wheel for Databricks (needs Docker)
```

Parity (from the repo root, then in `rust/`):

```bash
sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages src/test/resources/gfs_sample.grib2 target/parity"
.venv/bin/python scripts/parity.py ../src/test/resources/gfs_sample.grib2 ../target/parity [--whole-file]
```

Regenerate the embedded tables after a netCDF-Java upgrade:

```bash
sbt "Test/runMain com.example.spark.grib.tools.ExportGribTables rust/data"
```

Coverage: Rust 93% of lines (`cargo llvm-cov`), Python 93% (`pytest --cov`).

## On Databricks

`examples/databricks/rust_python_datasource_eval.py` runs both readers on a
UC volume and compares them (setup in `examples/databricks/README.md`).

Results (2026-10-24, DBR 16.4 LTS, dedicated cluster with 2 workers of
`Standard_D4ds_v5`, five GFS 1° files with `.idx` files in a UC volume,
3,668 messages; best of two runs):

| Query | Rust (Python data source) | Scala (DSv2) |
|---|---|---|
| Gridded, 2 m TMP only | 1.65 s | 1.75 s |
| Gridded, every message | 3.89 s | 4.07 s |
| Flat, 2 m TMP only | 1.02 s | 0.78 s |
| Flat, every message (239M rows) | 60.5 s | 19.4 s |

- **Parity on the cluster:** every column of all 3,668 gridded rows matches,
  values element by element (max relative difference 1.9e-7). Flat output
  matches on row counts, missing points, and lat/lon/value sums per file.
- **Direct reads on the driver**, no Spark: 2 m TMP from the five files in
  the volume (`/Volumes` FUSE) takes 2.4 s and reads 318 KB of 218 MB. Three
  fields from a 528 MB GFS 0.25° file on NOAA's S3 bucket take 0.6 s and read
  2.4 MB.
- **Full flat reads are 3× slower than Scala.** The cause is Spark taking
  rows from a Python data source, not the decoding. Rust builds the 45M flat
  rows of one file in 1.2 s, but Spark takes 8.7 s for that file end to end
  locally. Flat rows repeat about 100 bytes of per-field columns for every
  point, and a Python data source ships all of it as Arrow from the Python
  worker to the JVM. The Scala reader builds rows inside the JVM. For large
  flat reads, read gridded and `explode` the values in Spark, or keep the
  Scala reader.
