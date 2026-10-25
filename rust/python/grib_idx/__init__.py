"""GRIB2 reader: idx-driven byte-range reads from object storage, decoded in
Rust and returned as Arrow, in the same schemas as the Scala `grib` Spark
data source.

    import grib_idx
    table = grib_idx.read(
        "s3://noaa-gfs-bdp-pds/gfs.20240101/00/atmos/gfs.t00z.pgrb2.0p25.f006",
        variables=["TMP"],
        levels=["2 m above ground"],
        storage_options={"skip_signature": "true", "region": "us-east-1"},
    )

For Spark, see `grib_idx.spark`.
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from grib_idx._native import is_supported_predicate, list_files, plan_file, read_plan, schema

__all__ = ["FileStats", "is_supported_predicate", "list_files", "plan_file", "read", "read_plan", "read_with_stats", "schema"]


@dataclass(frozen=True)
class FileStats:
    """What reading one file did."""

    location: str
    file_size: int
    messages_in_index: int | None
    messages_read: int
    bytes_read: int


def read(
    path: str,
    *,
    variables: Sequence[str] | None = None,
    levels: Sequence[str] | None = None,
    level_type: str | None = None,
    predicate: tuple | None = None,
    output_mode: str = "gridded",
    longitude_range: str = "0to360",
    use_index: bool = True,
    storage_options: dict[str, str] | None = None,
) -> pa.Table:
    """Read the GRIB2 fields at `path` (a file, directory or glob) as a table.

    - `variables`: short names, matched case-insensitively against the decoded
      name or the `.idx` name.
    - `levels`: `.idx` level descriptions such as "2 m above ground" (exact).
    - `level_type`: case-insensitive substring of the decoded `level_type`.
    - `predicate`: a filter tuple, e.g. `("eq", "variable_short_name", "TMP")`
      (see `is_supported_predicate`).
    - `output_mode`: "gridded" (one row per field) or "flat" (one per point).
    - `storage_options`: object store options, e.g. credentials or
      `{"skip_signature": "true", "region": "us-east-1"}` for public buckets.
    """
    table, _ = read_with_stats(
        path,
        variables=variables,
        levels=levels,
        level_type=level_type,
        predicate=predicate,
        output_mode=output_mode,
        longitude_range=longitude_range,
        use_index=use_index,
        storage_options=storage_options,
    )
    return table


def read_with_stats(
    path: str,
    *,
    variables: Sequence[str] | None = None,
    levels: Sequence[str] | None = None,
    level_type: str | None = None,
    predicate: tuple | None = None,
    output_mode: str = "gridded",
    longitude_range: str = "0to360",
    use_index: bool = True,
    storage_options: dict[str, str] | None = None,
) -> tuple[pa.Table, list[FileStats]]:
    """As `read`, also returning what was read from each file."""
    selection = {
        "variables": list(variables) if variables is not None else None,
        "levels": list(levels) if levels is not None else None,
        "level_type": level_type,
        "predicate": predicate,
    }
    batches: list[pa.RecordBatch] = []
    stats: list[FileStats] = []
    for location, size, has_idx in list_files(path, storage_options):
        plan = plan_file(location, size, has_idx, use_index=use_index, storage_options=storage_options, **selection)
        warn_all(plan["warnings"])
        scan = read_plan(
            location,
            plan["file_size"],
            plan["messages"],
            output_mode=output_mode,
            longitude_range=longitude_range,
            storage_options=storage_options,
            **selection,
        )
        batches.extend(scan)
        read_stats = scan.stats
        warn_all(read_stats["decode_failures"], prefix=f"skipped undecodable GRIB message in {location}: ")
        stats.append(
            FileStats(location, plan["file_size"], plan["messages_in_index"], read_stats["messages_read"], read_stats["bytes_read"])
        )
    return pa.Table.from_batches(batches, schema=schema(output_mode)), stats


def warn_all(messages: Sequence[str], prefix: str = "") -> None:
    for message in messages:
        warnings.warn(prefix + message, stacklevel=3)
