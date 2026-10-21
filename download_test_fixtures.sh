#!/bin/bash
# Downloads small GRIB2 files for unit/integration tests from NOAA public S3 buckets.
# These are publicly accessible — no AWS credentials needed.

set -e

RESOURCES_DIR="src/test/resources"
mkdir -p "$RESOURCES_DIR"

echo "Downloading test fixtures to $RESOURCES_DIR ..."

# GFS 1.0° — single forecast hour (analysis), ~5MB — good for fast tests
echo "[1/7] GFS 1.0° analysis..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_sample.grib2" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000"

# Matching index file
echo "[2/7] GFS 1.0° index..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_sample.grib2.idx" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f000.idx"

# GFS Wave — very small (~1MB), good for quick smoke tests
echo "[3/7] GFS Wave (small file)..."
curl -sS --fail -o "$RESOURCES_DIR/gfs_wave_sample.grib2" \
  "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/wave/station/gfswave.t00z.atlocn.0p16.f000.grib2" \
  || echo "  WARN: GFS Wave download failed (non-critical)"

# NBM — National Blend of Models, ~10MB, good variety of variables
echo "[4/7] NBM core..."
curl -sS --fail -o "$RESOURCES_DIR/nbm_sample.grib2" \
  "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20240101/00/core/blend.t00z.core.f001.co.grib2" \
  || echo "  WARN: NBM download failed (non-critical)"

# GEFS-Aerosols — first 15 messages only (~2.6MB): one per aerosol species/size
# bin at hybrid level 1 (product template 4.48). Byte range and .idx are cut to
# the same 15 messages; message 16 starts at byte 2613582.
echo "[5/7] GEFS-Aerosols (first 15 messages)..."
GEFS_CHEM_URL="https://noaa-gefs-pds.s3.amazonaws.com/gefs.20240101/00/chem/pgrb2ap5/gefs.chem.t00z.a3d_0p50.f003.grib2"
curl -sS --fail -r 0-2613581 -o "$RESOURCES_DIR/gefs_chem_sample.grib2" "$GEFS_CHEM_URL"
curl -sS --fail "$GEFS_CHEM_URL.idx" | sed -n 1,15p > "$RESOURCES_DIR/gefs_chem_sample.grib2.idx"

# One message per GRIB2 product template the reader extracts into `product`,
# concatenated into a single file (~5MB). The .idx sidecar gives each message's
# byte range: from its offset to the next message's offset.
append_message() {
  local url=$1 message=$2 out=$3
  local range
  range=$(curl -sS --fail "$url.idx" | awk -F: -v n="$message" \
    '$1 == n { start = $2 } $1 == n + 1 { end = $2 - 1 } END { print start "-" end }')
  curl -sS --fail -r "$range" "$url" >> "$out"
}

echo "[6/7] Product-template sample (ensemble, probability, percentile, interval)..."
TEMPLATES_OUT="$RESOURCES_DIR/templates_sample.grib2"
GEFS="https://noaa-gefs-pds.s3.amazonaws.com/gefs.20240101/00/atmos/pgrb2ap5"
NBM_QMD="https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20240101/00/qmd/blend.t00z.qmd.f024.co.grib2"
GFS_F006="https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20240101/00/atmos/gfs.t00z.pgrb2.1p00.f006"
rm -f "$TEMPLATES_OUT"
append_message "$GEFS/gep01.t00z.pgrb2a.0p50.f006" 63 "$TEMPLATES_OUT"  # 0: TMP 2 m, member +1 (4.1)
append_message "$GEFS/gep01.t00z.pgrb2a.0p50.f006" 69 "$TEMPLATES_OUT"  # 1: APCP 0-6 h, member +1 (4.11)
append_message "$GEFS/geavg.t00z.pgrb2a.0p50.f006" 63 "$TEMPLATES_OUT"  # 2: TMP 2 m, ensemble mean (4.2)
append_message "$GEFS/geavg.t00z.pgrb2a.0p50.f006" 69 "$TEMPLATES_OUT"  # 3: APCP 0-6 h, ensemble mean (4.12)
append_message "$NBM_QMD" 1 "$TEMPLATES_OUT"                            # 4: APCP 18-24 h, prob >0.254 (4.9)
append_message "$NBM_QMD" 11 "$TEMPLATES_OUT"                           # 5: APCP 18-24 h, 1% level (4.10)
append_message "$GFS_F006" 596 "$TEMPLATES_OUT"                         # 6: APCP 0-6 h (4.8)

# RAP — first 17 messages (~0.9MB). Messages 12 and 17 each pack UGRD and VGRD
# into one GRIB message, listed in the .idx as sub-messages 12.1/12.2 and
# 17.1/17.2 at a shared byte offset. Message 18 starts at byte 916565.
echo "[7/7] RAP (multi-field messages)..."
RAP_URL="https://noaa-rap-pds.s3.amazonaws.com/rap.20240101/rap.t00z.awp130pgrbf00.grib2"
curl -sS --fail -r 0-916564 -o "$RESOURCES_DIR/rap_submessage_sample.grib2" "$RAP_URL"
curl -sS --fail "$RAP_URL.idx" | sed -n 1,19p > "$RESOURCES_DIR/rap_submessage_sample.grib2.idx"

echo ""
echo "Done. Files:"
ls -lh "$RESOURCES_DIR"/*.grib2 "$RESOURCES_DIR"/*.idx 2>/dev/null || true
