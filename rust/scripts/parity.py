"""Compare the Rust reader's output with the Scala/netCDF-Java reference dump.

Generate the reference first (from the repo root):

    sbt "Test/runMain com.example.spark.grib.tools.DumpDecodedMessages \\
         src/test/resources/<fixture>.grib2 target/parity"

then: python scripts/parity.py ../src/test/resources/<fixture>.grib2 ../target/parity [--whole-file]

Checks every gridded-mode column, every value, and the first message's
flat-mode latitudes/longitudes. `--whole-file` reads without the `.idx`.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa

from grib_idx import _native

EXACT_KEYS = ["discipline", "variable_short_name", "variable_name", "units", "level_type",
              "reference_time_ms", "forecast_time_ms", "pds_template", "nx", "ny"]
FLOAT_KEYS = ["level_value", "level_value2"]
GEOMETRY_KEYS = ["lat_start", "lon_start", "lat_increment", "lon_increment"]
# netCDF-Java and grib-rs implement the projections separately: NBM's Lambert
# origin differs by 0.3 m (1.1e-6 relative).
GEOMETRY_TOLERANCE = 1e-5
FLOAT_TOLERANCE = 1e-9
# Two float32 ULPs: both decoders unpack in different arithmetic.
VALUE_TOLERANCE = 2.4e-7
# Projected grids: NBM differs by up to 1.8e-5 degrees (2 m).
LAT_LON_TOLERANCE = 5e-5
MAX_REPORTED = 20
# Messages the reference (netCDF-Java 5.6) decodes wrongly, verified against
# ecCodes (pygrib), which agrees with the Rust reader exactly.
KNOWN_REFERENCE_DEFECTS = {
    ("nbm_sample.grib2", 227): "netCDF-Java marks 98% of PWTHER (complex packing 5.2, missing-value "
    "management 1) missing and mis-decodes the rest; ecCodes and Rust agree",
}


@dataclass
class Report:
    failures: list[str] = field(default_factory=list)
    known_defects: list[str] = field(default_factory=list)
    worst_value_diff: float = 0.0
    worst_geometry_diff: float = 0.0
    worst_lat_lon_diff: float = 0.0
    messages: int = 0
    values: int = 0

    def fail(self, index: int, message: str) -> None:
        self.failures.append(f"[{index}] {message}")


def close(a, b, tolerance: float) -> bool:
    if a is None or b is None or (isinstance(a, float) and math.isnan(a)) or (isinstance(b, float) and math.isnan(b)):
        as_missing = lambda v: v is None or (isinstance(v, float) and math.isnan(v))  # noqa: E731
        return as_missing(a) and as_missing(b)
    return abs(a - b) <= tolerance * max(1.0, abs(b))


def same_product(ref, actual) -> bool:
    if isinstance(ref, dict) and isinstance(actual, dict):
        return ref.keys() == actual.keys() and all(same_product(ref[k], actual[k]) for k in ref)
    if isinstance(ref, float) or isinstance(actual, float):
        return close(ref, actual, FLOAT_TOLERANCE)
    return ref == actual


def rust_batches(grib: Path, use_index: bool, output_mode: str, first_message_only: bool = False):
    for location, size, has_idx in _native.list_files(str(grib)):
        plan = _native.plan_file(location, size, has_idx, use_index=use_index)
        messages = plan["messages"][:1] if first_message_only and plan["messages"] is not None else plan["messages"]
        yield from _native.read_plan(location, plan["file_size"], messages, output_mode=output_mode)


def compare_row(row: dict, values: np.ndarray, ref: dict, expected: np.ndarray, report: Report) -> None:
    index = ref["message_index"]
    for key in EXACT_KEYS:
        actual = int(row[key[:-3]].timestamp() * 1000) if key.endswith("_ms") else row[key]
        if actual != ref[key]:
            report.fail(index, f"{key}: scala={ref[key]!r} rust={actual!r}")
    for key in FLOAT_KEYS:
        if not close(row[key], ref[key], FLOAT_TOLERANCE):
            report.fail(index, f"{key}: scala={ref[key]} rust={row[key]}")
    for key in GEOMETRY_KEYS:
        if not close(row[key], ref[key], GEOMETRY_TOLERANCE):
            report.fail(index, f"{key}: scala={ref[key]} rust={row[key]}")
        elif ref[key] is not None and row[key] is not None:
            report.worst_geometry_diff = max(report.worst_geometry_diff, abs(row[key] - ref[key]) / max(1.0, abs(ref[key])))
    if not same_product(ref["product"], row["product"]):
        report.fail(index, f"product: scala={ref['product']} rust={row['product']}")
    compare_values(index, values, expected, report)


def compare_values(index: int, actual: np.ndarray, expected: np.ndarray, report: Report) -> None:
    if actual.shape != expected.shape:
        report.fail(index, f"value count: scala={expected.size} rust={actual.size}")
        return
    if (mismatch := int(np.count_nonzero(np.isnan(actual) != np.isnan(expected)))):
        report.fail(index, f"{mismatch} points missing in one reader only")
    both = ~np.isnan(actual) & ~np.isnan(expected)
    if both.any():
        a, e = actual[both], expected[both].astype("f8")
        worst = float((np.abs(a - e) / np.maximum(np.abs(e), 1e-30)).max())
        report.worst_value_diff = max(report.worst_value_diff, worst)
        if worst > VALUE_TOLERANCE:
            report.fail(index, f"values differ by up to {worst:.3g} (relative)")
    report.values += actual.size


def compare_gridded(grib: Path, ref_dir: Path, use_index: bool, report: Report) -> None:
    refs = [json.loads(line) for line in (ref_dir / f"{grib.name}.jsonl").read_text().splitlines()]
    offsets = np.concatenate([[0], np.cumsum([r["num_values"] for r in refs])])
    ref_values = np.memmap(ref_dir / f"{grib.name}.f32", dtype="<f4", mode="r")
    by_index = {r["message_index"]: (r, offsets[i]) for i, r in enumerate(refs)}
    seen = set()
    for batch in rust_batches(grib, use_index, "gridded"):
        values = batch.column("values")
        flat = values.values.to_numpy(zero_copy_only=False)
        value_offsets = values.offsets.to_numpy()
        for i, row in enumerate(batch.drop_columns(["values"]).to_pylist()):
            index = row["message_index"]
            if index not in by_index:
                report.fail(index, "only in Rust output")
                continue
            seen.add(index)
            if (grib.name, index) in KNOWN_REFERENCE_DEFECTS:
                report.known_defects.append(f"[{index}] {KNOWN_REFERENCE_DEFECTS[(grib.name, index)]}")
                continue
            ref, start = by_index[index]
            expected = np.asarray(ref_values[start : start + ref["num_values"]])
            compare_row(row, flat[value_offsets[i] : value_offsets[i + 1]], ref, expected, report)
    report.failures += [f"[{i}] missing from Rust output" for i in sorted(set(by_index) - seen)]
    report.messages = len(seen)


def compare_first_lat_lons(grib: Path, ref_dir: Path, use_index: bool, report: Report) -> None:
    reference = np.fromfile(ref_dir / f"{grib.name}.latlon.f64", dtype="<f8")
    lats, lons = np.split(reference, 2)
    batch = next(b for b in rust_batches(grib, use_index, "flat", first_message_only=True) if b.num_rows)
    actual_lats = batch.column("latitude").to_numpy()
    actual_lons = batch.column("longitude").to_numpy()
    if actual_lats.shape != lats.shape:
        report.fail(0, f"flat point count: scala={lats.size} rust={actual_lats.size}")
        return
    lon_diff = np.abs((actual_lons - lons + 180.0) % 360.0 - 180.0)
    report.worst_lat_lon_diff = float(max(np.abs(actual_lats - lats).max(), lon_diff.max()))
    if report.worst_lat_lon_diff > LAT_LON_TOLERANCE:
        report.fail(0, f"flat lat/lon differ by up to {report.worst_lat_lon_diff:.3g} degrees")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("grib", type=Path)
    parser.add_argument("ref_dir", type=Path)
    parser.add_argument("--whole-file", action="store_true", help="read without the .idx sidecar")
    args = parser.parse_args()
    use_index = not args.whole_file
    report = Report()
    compare_gridded(args.grib, args.ref_dir, use_index, report)
    compare_first_lat_lons(args.grib, args.ref_dir, use_index, report)

    mode = "whole file" if args.whole_file else "indexed"
    print(f"{args.grib.name} ({mode}): {report.messages} messages, {report.values} values")
    print(f"  max relative value diff {report.worst_value_diff:.3g}, geometry {report.worst_geometry_diff:.3g}, "
          f"flat lat/lon {report.worst_lat_lon_diff:.3g} deg")
    for defect in report.known_defects:
        print(f"  KNOWN REFERENCE DEFECT {defect}")
    for failure in report.failures[:MAX_REPORTED]:
        print(f"  FAIL {failure}")
    if len(report.failures) > MAX_REPORTED:
        print(f"  ... {len(report.failures) - MAX_REPORTED} more failures")
    print("  PASS" if not report.failures else f"  {len(report.failures)} FAILURES")
    return 0 if not report.failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
