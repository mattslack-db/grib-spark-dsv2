"""Tests for option parsing and partition planning (ported from the Scala specs)."""

import pytest

from grib_idx.planning import (
    DEFAULT_MAX_PARTITION_BYTES,
    MIN_PARTITION_BYTES,
    GribOptions,
    group_messages,
    pack,
    parse_size,
    target_partition_bytes,
)


def message(index: int, length: int) -> tuple:
    return (index, index * 1000, length, [("TMP", "500 mb")])


def test_defaults_match_the_scala_reader() -> None:
    options = GribOptions.parse({"path": "/data/gfs"})

    assert options.paths == ["/data/gfs"]
    assert options.output_mode == "flat"
    assert options.longitude_range == "0to360"
    assert options.use_index
    assert options.max_partition_bytes == DEFAULT_MAX_PARTITION_BYTES
    assert options.variables is None and options.level_type is None


def test_options_are_case_insensitive_and_parsed() -> None:
    options = GribOptions.parse({
        "PATH": "/x",
        "Variables": "TMP, RH,,",
        "outputmode": "GRIDDED",
        "messagesPerPartition": "4",
        "useIndex": "false",
        "levelType": "  isobaric ",
        "storage.skip_signature": "true",
    })

    assert options.variables == ["TMP", "RH"]
    assert options.output_mode == "gridded"
    assert options.messages_per_partition == 4
    assert not options.use_index
    assert options.level_type == "isobaric"
    assert options.storage_options == {"skip_signature": "true"}


def test_paths_option_accepts_a_json_list() -> None:
    assert GribOptions.parse({"paths": '["/a", "/b"]'}).paths == ["/a", "/b"]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"outputMode": "wide"}, "outputMode"),
        ({"longitudeRange": "east"}, "longitudeRange"),
        ({"messagesPerPartition": "0"}, "messagesPerPartition"),
        ({"maxPartitionBytes": "lots"}, "maxPartitionBytes"),
    ],
)
def test_invalid_options_raise(options: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        GribOptions.parse(options)


def test_sizes_use_spark_syntax() -> None:
    assert parse_size("134217728") == 134217728
    assert parse_size("64m") == 64 * 1024**2
    assert parse_size("1G") == 1024**3


def test_target_bytes_spread_over_three_partitions_per_core_within_bounds() -> None:
    assert target_partition_bytes(600 * 1024**2, 8, DEFAULT_MAX_PARTITION_BYTES) == 25 * 1024**2
    assert target_partition_bytes(1024, 8, DEFAULT_MAX_PARTITION_BYTES) == MIN_PARTITION_BYTES
    assert target_partition_bytes(10 * 1024**3, 1, 64 * 1024**2) == 64 * 1024**2


def test_pack_groups_in_file_order_and_isolates_oversized_messages() -> None:
    messages = [message(0, 40), message(1, 40), message(2, 40), message(3, 500), message(4, 10)]

    groups = pack(messages, target_bytes=100)

    assert [[m[0] for m in g] for g in groups] == [[0, 1], [2], [3], [4]]


def test_messages_per_partition_overrides_byte_packing() -> None:
    options = GribOptions.parse({"path": "/x", "messagesPerPartition": "2"})
    messages = [message(i, 10) for i in range(5)]

    assert [len(g) for g in group_messages(messages, options, target_bytes=1)] == [2, 2, 1]
