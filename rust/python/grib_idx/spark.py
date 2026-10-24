"""PySpark Python data source over the Rust reader, with the Scala `grib`
data source's options and schemas:

    from grib_idx.spark import GribIdxDataSource
    spark.dataSource.register(GribIdxDataSource)
    df = (spark.read.format("grib_idx")
          .option("outputMode", "gridded")
          .option("variables", "TMP")
          .load("/Volumes/catalog/schema/volume/gfs/"))

Options (case-insensitive): `variables`, `levelType`, `outputMode` (default
`flat`), `messagesPerPartition`, `maxPartitionBytes`, `longitudeRange`,
`useIndex`, as in the Scala reader, plus:

- `levels`: comma-separated `.idx` level descriptions ("2 m above ground").
- `storage.<key>`: object store options, e.g. `storage.skip_signature`.
- `parallelism`: cores to size partitions for (default: this machine's CPU
  count, as a Python data source can't see the cluster's).
- `filterPushdown`: "true" to push SQL filters into the reader (Spark 4.1+,
  with `spark.sql.python.filterPushdown.enabled=true`).
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

from grib_idx import _native, warn_all
from grib_idx.planning import GribOptions, Message, group_messages, target_partition_bytes

try:  # Filter classes exist from Spark 4.1.
    from pyspark.sql.datasource import (
        EqualTo,
        Filter,
        GreaterThan,
        GreaterThanOrEqual,
        In,
        LessThan,
        LessThanOrEqual,
        Not,
    )
except ImportError:  # pragma: no cover - Spark < 4.1
    Filter = None

# Files planned concurrently: planning a file is a few storage round trips.
PLANNING_THREADS = 16

_COMPARISONS = {"GreaterThan": "gt", "GreaterThanOrEqual": "ge", "LessThan": "lt", "LessThanOrEqual": "le"}


def _ddl_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_int32(arrow_type):
        return "INT"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE"
    if pa.types.is_string(arrow_type):
        return "STRING"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_list(arrow_type):
        return f"ARRAY<{_ddl_type(arrow_type.value_type)}>"
    if pa.types.is_struct(arrow_type):
        return "STRUCT<" + ", ".join(f"`{f.name}`: {_ddl_type(f.type)}" for f in arrow_type) + ">"
    raise TypeError(f"no Spark type for {arrow_type}")


def spark_schema(output_mode: str) -> str:
    """The Spark DDL of the reader's Arrow schema for `output_mode`."""
    return ", ".join(f"`{f.name}` {_ddl_type(f.type)}" for f in _native.schema(output_mode))


@dataclass(frozen=True)
class GribPartition(InputPartition):
    """A file's planned messages (or None: the whole file)."""

    location: str
    file_size: int
    messages: list[Message] | None


class GribIdxReader(DataSourceReader):
    def __init__(self, options: GribOptions) -> None:
        self.options = options
        self.predicate: tuple | None = None

    def _selection(self) -> dict:
        o = self.options
        return {"variables": o.variables, "levels": o.levels, "level_type": o.level_type, "predicate": self.predicate}

    def partitions(self) -> Sequence[GribPartition]:
        o = self.options
        files = [f for path in o.paths for f in _native.list_files(path, o.storage_options)]
        with ThreadPoolExecutor(max_workers=max(1, min(PLANNING_THREADS, len(files)))) as pool:
            plans = list(pool.map(self._plan, files))
        for plan in plans:
            warn_all(plan["warnings"])
        total_bytes = sum(plan_bytes(p) for p in plans)
        target = target_partition_bytes(total_bytes, o.parallelism or os.cpu_count() or 1, o.max_partition_bytes)
        partitions: list[GribPartition] = []
        for plan in plans:
            if plan["messages"] is None:
                partitions.append(GribPartition(plan["location"], plan["file_size"], None))
            else:
                for group in group_messages(plan["messages"], o, target):
                    partitions.append(GribPartition(plan["location"], plan["file_size"], group))
        return partitions

    def _plan(self, file: tuple[str, int, bool | None]) -> dict:
        location, size, has_idx = file
        plan = _native.plan_file(location, size, has_idx, use_index=self.options.use_index, storage_options=self.options.storage_options, **self._selection())
        return {**plan, "location": location}

    def read(self, partition: GribPartition) -> Iterator[pa.RecordBatch]:
        o = self.options
        scan = _native.read_plan(
            partition.location,
            partition.file_size,
            partition.messages,
            output_mode=o.output_mode,
            longitude_range=o.longitude_range,
            storage_options=o.storage_options,
            **self._selection(),
        )
        yield from (b for b in scan if b.num_rows)
        warn_all(scan.stats["decode_failures"], prefix=f"skipped undecodable GRIB message in {partition.location}: ")


def plan_bytes(plan: dict) -> int:
    messages = plan["messages"]
    return plan["file_size"] if messages is None else sum(m[2] for m in messages)


def to_predicate(f: Filter) -> tuple | None:
    """The reader's predicate tuple for a Spark filter, or None if it can't be
    evaluated exactly (then Spark keeps it)."""
    if isinstance(f, Not):
        child = to_predicate(f.child)
        return None if child is None else ("not", child)
    if len(getattr(f, "attribute", ())) != 1:
        return None
    column = f.attribute[0]
    if isinstance(f, EqualTo):
        candidate = ("eq", column, f.value)
    elif isinstance(f, In):
        candidate = ("in", column, list(f.value))
    elif type(f).__name__ in _COMPARISONS and isinstance(f, (GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual)):
        candidate = (_COMPARISONS[type(f).__name__], column, f.value)
    else:
        return None
    return candidate if _native.is_supported_predicate(candidate) else None


class GribIdxPushdownReader(GribIdxReader):
    """Pushes the filters the reader evaluates exactly; Spark keeps the rest."""

    def pushFilters(self, filters: list[Filter]) -> Iterator[Filter]:  # noqa: N802 - Spark's API name
        for f in filters:
            predicate = to_predicate(f)
            if predicate is None:
                yield f
            else:
                self.predicate = predicate if self.predicate is None else ("and", self.predicate, predicate)


class GribIdxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "grib_idx"

    def schema(self) -> str:
        return spark_schema(GribOptions.parse(self.options).output_mode)

    def reader(self, schema) -> GribIdxReader:  # noqa: ANN001 - Spark passes a StructType
        options = GribOptions.parse(self.options)
        if self.options.get("filterPushdown", "false").lower() == "true":
            if Filter is None:
                raise ValueError("filterPushdown needs Spark 4.1 or later")
            return GribIdxPushdownReader(options)
        return GribIdxReader(options)
