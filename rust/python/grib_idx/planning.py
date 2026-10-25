"""Reader options and partition planning for the Spark data source, ported
from `GribOptions.scala` and `PartitionPlanning.scala`. Pure Python, so it is
testable without Spark."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

OUTPUT_MODES = ("flat", "gridded")
LONGITUDE_RANGES = ("0to360", "-180to180")
DEFAULT_OUTPUT_MODE = "flat"
DEFAULT_LONGITUDE_RANGE = "0to360"
# As Spark's spark.sql.files.maxPartitionBytes.
DEFAULT_MAX_PARTITION_BYTES = 128 * 1024 * 1024
# Smallest byte-based partition, so many tiny messages aren't spread one or
# two per task (plays the role of spark.sql.files.openCostInBytes).
MIN_PARTITION_BYTES = 4 * 1024 * 1024
# Partitions to aim for per core, so uneven sizes leave cores idle for at most
# about a third of a round.
PARTITIONS_PER_CORE = 3
# Options prefixed with this are passed to the object store (e.g.
# `storage.skip_signature`), since there is no Hadoop configuration to read.
STORAGE_OPTION_PREFIX = "storage."

_SIZE_UNITS = {"": 1, "b": 1, "k": 1024, "kb": 1024, "m": 1024**2, "mb": 1024**2, "g": 1024**3, "gb": 1024**3, "t": 1024**4, "tb": 1024**4}
_SIZE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]*)\s*$")


def parse_size(text: str) -> int:
    """Bytes from a Spark-style size: "134217728", "64m", "128MB", "1g"."""
    match = _SIZE.match(text)
    unit = match.group(2).lower() if match else None
    if match is None or unit not in _SIZE_UNITS:
        raise ValueError(f"Invalid maxPartitionBytes '{text}'. Use a number of bytes or a size such as '64m' or '1g'.")
    size = int(match.group(1)) * _SIZE_UNITS[unit]
    if size <= 0:
        raise ValueError(f"maxPartitionBytes must be > 0, got {size}")
    return size


def _split(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def _paths(options: Mapping[str, str]) -> list[str]:
    raw = options.get("paths")
    if raw:
        try:
            parsed = json.loads(raw)
            return [str(p) for p in parsed] if isinstance(parsed, list) else [str(parsed)]
        except json.JSONDecodeError:
            return [p.strip().strip('"') for p in raw.strip("[]").split(",") if p.strip()]
    return [options["path"]] if options.get("path") else []


@dataclass(frozen=True)
class GribOptions:
    paths: list[str]
    variables: list[str] | None = None
    levels: list[str] | None = None
    level_type: str | None = None
    output_mode: str = DEFAULT_OUTPUT_MODE
    messages_per_partition: int | None = None
    max_partition_bytes: int = DEFAULT_MAX_PARTITION_BYTES
    longitude_range: str = DEFAULT_LONGITUDE_RANGE
    use_index: bool = True
    parallelism: int | None = None
    storage_options: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def parse(options: Mapping[str, str]) -> GribOptions:
        """Parse data source options (keys are case-insensitive)."""
        lower = {k.lower(): v for k, v in options.items()}
        output_mode = lower.get("outputmode", DEFAULT_OUTPUT_MODE).strip().lower()
        if output_mode not in OUTPUT_MODES:
            raise ValueError(f"Invalid outputMode '{output_mode}'. Must be 'flat' or 'gridded'.")
        longitude_range = lower.get("longituderange", DEFAULT_LONGITUDE_RANGE).strip()
        if longitude_range not in LONGITUDE_RANGES:
            raise ValueError(f"Invalid longitudeRange '{longitude_range}'. Must be '0to360' or '-180to180'.")
        messages_per_partition = lower.get("messagesperpartition")
        if messages_per_partition is not None:
            messages_per_partition = int(messages_per_partition)
            if messages_per_partition <= 0:
                raise ValueError(f"messagesPerPartition must be > 0, got {messages_per_partition}")
        parallelism = lower.get("parallelism")
        level_type = (lower.get("leveltype") or "").strip() or None
        return GribOptions(
            paths=_paths(lower),
            variables=_split(lower.get("variables")),
            levels=_split(lower.get("levels")),
            level_type=level_type,
            output_mode=output_mode,
            messages_per_partition=messages_per_partition,
            max_partition_bytes=parse_size(lower["maxpartitionbytes"]) if "maxpartitionbytes" in lower else DEFAULT_MAX_PARTITION_BYTES,
            longitude_range=longitude_range,
            use_index=lower.get("useindex", "true").strip().lower() == "true",
            parallelism=int(parallelism) if parallelism else None,
            storage_options={k[len(STORAGE_OPTION_PREFIX) :]: v for k, v in lower.items() if k.startswith(STORAGE_OPTION_PREFIX)},
        )


def target_partition_bytes(total_bytes: int, parallelism: int, max_partition_bytes: int) -> int:
    """Spread the data over PARTITIONS_PER_CORE partitions per core, within
    [MIN_PARTITION_BYTES, max_partition_bytes]."""
    partitions = max(parallelism, 1) * PARTITIONS_PER_CORE
    per_partition = -(-total_bytes // partitions)
    return min(max_partition_bytes, max(MIN_PARTITION_BYTES, per_partition))


Message = tuple  # (message_index, offset, length, [(idx variable, idx level), ...])


def pack(messages: Sequence[Message], target_bytes: int) -> list[list[Message]]:
    """Group a file's messages (in file order) into partitions of up to
    `target_bytes`; a message larger than the target gets its own."""
    groups: list[list[Message]] = []
    current: list[Message] = []
    current_bytes = 0
    for message in messages:
        length = message[2]
        if current and current_bytes + length > target_bytes:
            groups.append(current)
            current, current_bytes = [], 0
        current = [*current, message]
        current_bytes += length
    return [*groups, current] if current else groups


def group_messages(messages: Sequence[Message], options: GribOptions, target_bytes: int) -> list[list[Message]]:
    if options.messages_per_partition is not None:
        n = options.messages_per_partition
        return [list(messages[i : i + n]) for i in range(0, len(messages), n)]
    return pack(messages, target_bytes)
