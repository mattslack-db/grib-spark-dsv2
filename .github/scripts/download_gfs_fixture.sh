#!/usr/bin/env bash
# Downloads the small GFS 1° sample and its .idx that the CI test jobs read.
# Run from the repo root.
set -euo pipefail

BASE="https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000"
mkdir -p src/test/resources
curl -sS --fail --retry 3 -o src/test/resources/gfs_sample.grib2 "$BASE"
curl -sS --fail --retry 3 -o src/test/resources/gfs_sample.grib2.idx "$BASE.idx"
