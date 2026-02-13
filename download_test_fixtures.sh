#!/bin/bash
# Downloads small GRIB2 files for unit/integration tests from NOAA public S3 buckets.
# These are publicly accessible — no AWS credentials needed.

set -e

RESOURCES_DIR="src/test/resources"
mkdir -p "$RESOURCES_DIR"

echo "Downloading test fixtures to $RESOURCES_DIR ..."

# GFS 1.0° — single forecast hour (analysis), ~5MB — good for fast tests
echo "[1/4] GFS 1.0° analysis..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_sample.grib2" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000"

# Matching index file
echo "[2/4] GFS 1.0° index..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_sample.grib2.idx" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000.idx"

# GFS Wave — very small (~1MB), good for quick smoke tests
echo "[3/4] GFS Wave (small file)..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_wave_sample.grib2" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/wave/station/gfswave.t00z.atlocn.0p16.f000.grib2" \
  || echo "  WARN: GFS Wave download failed (non-critical)"

# NBM — National Blend of Models, ~10MB, good variety of variables
echo "[4/4] NBM core..."
curl -sS --fail -o "$RESOURCES_DIR/nbm_sample.grib2" \
  "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20240101/00/core/blend.t00z.core.f001.co.grib2" \
  || echo "  WARN: NBM download failed (non-critical)"

echo ""
echo "Done. Files:"
ls -lh "$RESOURCES_DIR"/*.grib2 "$RESOURCES_DIR"/*.idx 2>/dev/null || true
