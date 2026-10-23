# grib-idx: Rust evaluation spike

Can the Scala reader's indexed lookup be rewritten in Rust and called from
Python, still reading only the needed bytes from object storage?

`grib_idx.read(location, variables=None, levels=None, storage_options=None)`
fetches the `.idx` sidecar, selects matching messages, range-reads only their
bytes (via [`object_store`](https://crates.io/crates/object_store): S3, ADLS,
GCS, HTTP, local/`/Volumes` paths), decodes them in parallel with
[grib-rs](https://crates.io/crates/grib), and returns a `pyarrow.RecordBatch`
with one row per field (the Scala reader's gridded layout).

```python
import grib_idx

batch, stats = grib_idx.read_with_stats(
    "s3://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f006",
    variables=["TMP", "UGRD", "VGRD"],
    levels=["2 m above ground", "10 m above ground"],
    storage_options={"skip_signature": "true", "region": "us-east-1"},
)
```

## Running

```bash
cd rust
cargo test --release                      # unit + fixture tests (fixtures: ../download_test_fixtures.sh)
uv venv .venv && uv pip install --python .venv/bin/python maturin numpy pytest pyarrow
.venv/bin/maturin develop --release       # builds and installs the extension
.venv/bin/python -m pytest tests/test_python.py
```

Parity against the Scala/netCDF-Java decoder:

```bash
# from the repo root: dump the reference for a fixture
sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages src/test/resources/gfs_sample.grib2 target/parity"
cd rust && .venv/bin/python scripts/parity.py ../src/test/resources/gfs_sample.grib2 ../target/parity
```

## Findings (2026-10-24)

**Values match netCDF-Java.** Over all 730 messages of the three indexed
fixtures (GFS 1°, GEFS-Aerosols with template 4.48, RAP with sub-messages;
52M values), every value agrees to within 1 float32 ULP (max relative
difference 1.19e-7), with identical missing-value masks. Discipline, grid
shape, level values, reference/forecast times, product template and
regular-grid origin/increments match exactly.

**Indexed reads work against object storage.** GFS 0.25° f006 on public S3
(528 MB, 743 messages): selecting 2 m TMP and 10 m UGRD/VGRD fetched 3
messages, 2.4 MB (0.46% of the file), in ~2 s end to end from the UK to
`us-east-1`. Decoding all 696 messages of the 1° GFS fixture locally takes
~0.2 s (release build, M-series Mac).

**Gaps found:**

| Gap | Detail | Fix |
|---|---|---|
| NCEP local code tables | grib-rs ships WMO tables only, so codes 192..254 (ozone mixing ratio, reflectivity, PBL, ...) have no name: 74/696 GFS long names and 13 level types are null | Port the names the Scala reader gets from netCDF-Java, or generate from NCEP's tables |
| Short names | Taken from the `.idx` (wgrib2 naming); netCDF-Java differs for 24/696 GFS fields (`CLMR` vs `CLWMR`, `LFTX` vs `LFT X`). Files without an `.idx` would need an abbreviation table | Pick one convention; ship an abbreviation table for idx-less files |
| GRIB1 | grib-rs decodes GRIB2 only | Out of scope unless GRIB1 sources matter |
| Projected grids | Origin/increments only for regular lat/lon (template 3.0); Lambert (NBM, RAP) left null | `gridpoints-proj` feature or port the projection maths |
| Product attributes | Template-specific columns (`product.*`, aerosol type names, units) not ported | Port `ProductAttributes` / `ParameterUnits` |
| Values type | `list<float32>` (native precision) vs the Scala schema's `array<double>` | Decide at integration time |

**Not yet evaluated:** UC Volumes on Databricks (FUSE path vs direct cloud
URLs with vended credentials), a Spark integration (PySpark Python Data
Source calling this per partition), head-to-head throughput against the Scala
reader, the NBM and templates fixtures (no `.idx`), and Linux wheel builds.
The build uses grib-rs's pure-Rust codecs (hayro JPEG2000, rust-aec), so
wheels need no C toolchain; their speed against openjpeg/libaec is untested.
