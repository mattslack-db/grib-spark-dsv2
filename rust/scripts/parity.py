"""Compare the Rust reader's output with the Scala/netCDF-Java reference dump.

Generate the reference first (from the repo root):

    sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages \
         src/test/resources/<fixture>.grib2 target/parity"

then run: python scripts/parity.py ../src/test/resources/<fixture>.grib2 ../target/parity
"""

import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np

import grib_idx

# Metadata compared exactly (or within FLOAT_TOLERANCE for floats).
EXACT_KEYS = ["discipline", "nx", "ny", "reference_time_ms", "forecast_time_ms", "pds_template"]
FLOAT_KEYS = ["level_value", "level_value2"]
# Grid geometry: netCDF-Java reports projected km for non lat/lon grids.
GEOMETRY_KEYS = ["lat_start", "lon_start", "lat_increment", "lon_increment"]
FLOAT_TOLERANCE = 1e-6
# Name columns differ by table source (netCDF-Java vs idx / grib-rs); report, don't fail.
NAME_KEYS = [("variable_short_name", "variable_short_name"), ("variable_name", "variable_name"), ("level_type", "level_type")]


def load_reference(grib_path: Path, ref_dir: Path) -> tuple[list[dict], np.ndarray]:
    meta = [json.loads(line) for line in (ref_dir / f"{grib_path.name}.jsonl").read_text().splitlines()]
    values = np.fromfile(ref_dir / f"{grib_path.name}.f32", dtype="<f4")
    return meta, values


def rust_rows(grib_path: Path) -> list[dict]:
    batch = grib_idx.read(str(grib_path))
    rows = batch.drop_columns(["values"]).to_pylist()
    values = batch.column("values")
    for row, grid in zip(rows, values):
        row["values"] = grid.values.to_numpy(zero_copy_only=False)
        for key in ("reference_time", "forecast_time"):
            ts = row.pop(key)
            row[f"{key}_ms"] = None if ts is None else int(ts.timestamp() * 1000)
    return rows


def floats_differ(a, b) -> bool:
    if a is None or b is None:
        return a is not b
    return abs(a - b) > FLOAT_TOLERANCE


def compare(grib_path: Path, ref_dir: Path) -> bool:
    ref_meta, ref_values = load_reference(grib_path, ref_dir)
    rows = {row["message_index"]: row for row in rust_rows(grib_path)}
    failures: list[str] = []
    name_diffs: Counter = Counter()
    worst_abs, worst_rel = 0.0, 0.0
    offset = 0

    for ref in ref_meta:
        index, n = ref["message_index"], ref["num_values"]
        expected = ref_values[offset : offset + n]
        offset += n
        row = rows.get(index)
        if row is None:
            failures.append(f"[{index}] missing from Rust output")
            continue

        failures += [f"[{index}] {k}: scala={ref[k]} rust={row[k]}" for k in EXACT_KEYS if ref[k] != row[k]]
        failures += [f"[{index}] {k}: scala={ref[k]} rust={row[k]}" for k in FLOAT_KEYS if floats_differ(ref[k], row[k])]
        if row["grid_template"] == 0:
            failures += [
                f"[{index}] {k}: scala={ref[k]} rust={row[k]}" for k in GEOMETRY_KEYS if floats_differ(ref[k], row[k])
            ]
        name_diffs.update(
            f"{rk}: {ref[sk]!r} vs {row[rk]!r}" for sk, rk in NAME_KEYS if ref[sk] != row[rk]
        )

        actual = row["values"]
        if actual.shape != expected.shape:
            failures.append(f"[{index}] value count: scala={expected.size} rust={actual.size}")
            continue
        nan_mismatch = int(np.count_nonzero(np.isnan(actual) != np.isnan(expected)))
        if nan_mismatch:
            failures.append(f"[{index}] {nan_mismatch} points missing in one reader only")
        both = ~np.isnan(actual) & ~np.isnan(expected)
        if both.any():
            diff = np.abs(actual[both].astype("f8") - expected[both].astype("f8"))
            scale = np.maximum(np.abs(expected[both].astype("f8")), 1e-30)
            worst_abs = max(worst_abs, float(diff.max()))
            worst_rel = max(worst_rel, float((diff / scale).max()))

    extra = set(rows) - {m["message_index"] for m in ref_meta}
    failures += [f"[{i}] only in Rust output" for i in sorted(extra)]

    print(f"{grib_path.name}: {len(ref_meta)} messages, {offset} values")
    print(f"  max |diff| = {worst_abs:.3g}, max relative diff = {worst_rel:.3g}")
    for diff, count in name_diffs.most_common(10):
        print(f"  name difference x{count}: {diff}")
    if len(name_diffs) > 10:
        print(f"  ... {len(name_diffs) - 10} more distinct name differences")
    for failure in failures[:20]:
        print(f"  FAIL {failure}")
    if len(failures) > 20:
        print(f"  ... {len(failures) - 20} more failures")
    print("  PASS" if not failures else f"  {len(failures)} FAILURES")
    return not failures


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    sys.exit(0 if compare(Path(sys.argv[1]), Path(sys.argv[2])) else 1)
