# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import struct
import zlib
from os import path
from pathlib import Path

import pytest
from pyroaring import BitMap

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.deletion_vector import (
    _DV_BLOB_MAGIC_NUMBER,
    DELETION_VECTOR_MAGIC,
    DELETION_VECTOR_V1_BLOB_TYPE,
    MAX_POSITION,
    PROPERTY_REFERENCED_DATA_FILE,
    ROW_POSITION_FIELD_ID,
    DeletionVector,
    _deserialize_dv_blob,
    has_deletion_vector_content_reference,
    read_deletion_vectors,
    write_deletion_vectors,
)
from pyiceberg.table.puffin import PuffinFile
from pyiceberg.typedef import Record


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/bitmaps/{file}", "rb") as f:
        return f.read()


def _dv_blob(bitmap_payload: bytes) -> bytes:
    bitmap_data = struct.pack("<I", _DV_BLOB_MAGIC_NUMBER) + bitmap_payload
    return struct.pack(">I", len(bitmap_data)) + bitmap_data + struct.pack(">I", zlib.crc32(bitmap_data) & 0xFFFFFFFF)


def _bitmap_payload() -> bytes:
    return (1).to_bytes(8, byteorder="little") + (0).to_bytes(4, byteorder="little") + BitMap([1, 3, 5]).serialize()


def test_deserialize_deletion_vector_blob() -> None:
    actual = _deserialize_dv_blob(_dv_blob(_bitmap_payload()), record_count=3)

    assert actual == [BitMap([1, 3, 5])]


def test_deserialize_deletion_vector_blob_invalid_length() -> None:
    with pytest.raises(ValueError, match="Invalid bitmap data length"):
        _deserialize_dv_blob(_dv_blob(_bitmap_payload())[:-1])


def test_deserialize_deletion_vector_blob_invalid_magic() -> None:
    bitmap_data = struct.pack("<I", _DV_BLOB_MAGIC_NUMBER + 1) + _bitmap_payload()
    blob = struct.pack(">I", len(bitmap_data)) + bitmap_data + struct.pack(">I", zlib.crc32(bitmap_data) & 0xFFFFFFFF)

    with pytest.raises(ValueError, match="Invalid magic number"):
        _deserialize_dv_blob(blob)


def test_deserialize_deletion_vector_blob_invalid_crc() -> None:
    blob = bytearray(_dv_blob(_bitmap_payload()))
    blob[-1] ^= 1

    with pytest.raises(ValueError, match="Invalid CRC"):
        _deserialize_dv_blob(bytes(blob))


def test_deserialize_deletion_vector_blob_invalid_cardinality() -> None:
    with pytest.raises(ValueError, match="Invalid cardinality"):
        _deserialize_dv_blob(_dv_blob(_bitmap_payload()), record_count=4)


def _data_file(
    content_offset: int | None = None,
    content_size_in_bytes: int | None = None,
    referenced_data_file: str | None = None,
) -> DataFile:
    return DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path="deletes.puffin",
        file_format=FileFormat.PUFFIN,
        record_count=1,
        content_offset=content_offset,
        content_size_in_bytes=content_size_in_bytes,
        referenced_data_file=referenced_data_file,
    )


def test_has_deletion_vector_content_reference() -> None:
    assert not has_deletion_vector_content_reference(_data_file())
    assert has_deletion_vector_content_reference(_data_file(content_offset=0))
    assert has_deletion_vector_content_reference(_data_file(content_size_in_bytes=1))
    assert has_deletion_vector_content_reference(_data_file(referenced_data_file="data.parquet"))


def test_read_deletion_vector_rejects_truncated_range(tmp_path: Path) -> None:
    delete_file_path = str(tmp_path / "deletes.puffin")
    blob = _dv_blob(_bitmap_payload())
    with open(delete_file_path, "wb") as f:
        f.write(blob)

    dv = DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_file_path,
        file_format=FileFormat.PUFFIN,
        record_count=3,
        referenced_data_file="data.parquet",
        content_offset=0,
        content_size_in_bytes=len(blob) + 8,
    )

    with pytest.raises(ValueError, match="Could not read deletion vector"):
        read_deletion_vectors(PyArrowFileIO(), dv)


def test_read_deletion_vector_requires_complete_content_reference() -> None:
    with pytest.raises(ValueError, match="content size is missing"):
        read_deletion_vectors(PyArrowFileIO(), _data_file(content_offset=0, referenced_data_file="data.parquet"))


def test_map_empty() -> None:
    puffin = _open_file("64mapempty.bin")

    expected: list[BitMap] = []
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_bitvals() -> None:
    puffin = _open_file("64map32bitvals.bin")

    expected = [BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_spread_vals() -> None:
    puffin = _open_file("64mapspreadvals.bin")

    expected = [
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ]
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_declared_count_exceeds_payload() -> None:
    # A truncated payload that claims a large number of bitmaps must be rejected,
    # rather than driving the deserialization loop on data that is not there.
    puffin = (2**32).to_bytes(8, byteorder="little") + b"\x00\x00\x00\x00"

    with pytest.raises(ValueError, match="Payload declares 4294967296 bitmaps, but only holds 4 bytes"):
        _ = DeletionVector._deserialize_bitmap(puffin)


def test_map_high_vals() -> None:
    puffin = _open_file("64maphighvals.bin")

    with pytest.raises(ValueError, match="Key 4022190063 is too large, max 2147483647 to maintain compatibility with Java impl"):
        _ = DeletionVector._deserialize_bitmap(puffin)


@pytest.mark.parametrize(
    "positions",
    [
        [1, 2, 3],
        [0],
        [3, 1, 2, 1, 3],  # unordered with duplicates
        [0, 1, 5, (1 << 32) + 7, (3 << 32) + 4],  # spread across bitmap keys, with a gap
    ],
)
def test_serialize_round_trips(positions: list[int]) -> None:
    dv = DeletionVector.from_positions("file.parquet", positions)

    serialized = dv.serialize()
    deserialized = DeletionVector("file.parquet", DeletionVector._deserialize_bitmap(serialized))
    assert deserialized.serialize() == serialized
    assert deserialized.to_vector().to_pylist() == sorted(set(positions))
    assert dv.cardinality == len(set(positions))


def test_serialize_skips_empty_bitmaps() -> None:
    serialized = DeletionVector.from_positions("file.parquet", [1, (2 << 32) + 1]).serialize()

    # Two non-empty bitmaps (keys 0 and 2); the empty bitmap for key 1 is omitted
    assert int.from_bytes(serialized[0:8], "little") == 2
    assert int.from_bytes(serialized[8:12], "little") == 0


def test_serialize_run_optimizes_contiguous_run() -> None:
    dv = DeletionVector.from_positions("file.parquet", range(100_000))

    serialized = dv.serialize()
    assert len(serialized) < 1_000  # ~16 KB without run-length encoding
    assert DeletionVector._deserialize_bitmap(serialized) == dv._bitmaps


def test_from_positions_rejects_negative() -> None:
    with pytest.raises(ValueError, match=f"Invalid position: -1, must be between 0 and {MAX_POSITION}"):
        DeletionVector.from_positions("file.parquet", [1, -1, 2])


def test_from_positions_rejects_out_of_range() -> None:
    too_large = MAX_POSITION + 1
    with pytest.raises(ValueError, match=f"Invalid position: {too_large}, must be between 0 and {MAX_POSITION}"):
        DeletionVector.from_positions("file.parquet", [1, too_large, 2])


def test_from_positions_rejects_empty() -> None:
    with pytest.raises(ValueError, match="Deletion vector must contain at least one position"):
        DeletionVector.from_positions("file.parquet", [])


def test_union() -> None:
    left = DeletionVector.from_positions("file.parquet", [1, 2])
    right = DeletionVector.from_positions("file.parquet", [2, 3, (1 << 32) + 1])

    union = left.union(right)

    assert union.to_vector().to_pylist() == [1, 2, 3, (1 << 32) + 1]
    assert union.cardinality == 4
    # The operands are unchanged
    assert left.to_vector().to_pylist() == [1, 2]


def test_union_rejects_different_data_files() -> None:
    with pytest.raises(ValueError, match="Cannot union deletion vectors of a.parquet and b.parquet"):
        DeletionVector.from_positions("a.parquet", [1]).union(DeletionVector.from_positions("b.parquet", [1]))


def test_to_blob_metadata() -> None:
    blob = DeletionVector.from_positions("s3://bucket/file.parquet", [1, 2, 3, 3]).to_blob()

    assert blob.metadata.type == DELETION_VECTOR_V1_BLOB_TYPE
    assert blob.metadata.fields == [ROW_POSITION_FIELD_ID]
    assert blob.metadata.properties == {PROPERTY_REFERENCED_DATA_FILE: "s3://bucket/file.parquet", "cardinality": "3"}
    assert blob.metadata.compression_codec is None
    # The snapshot id and sequence number are inherited at commit time, as in Java's BaseDVFileWriter
    assert blob.metadata.snapshot_id == -1
    assert blob.metadata.sequence_number == -1


def test_to_blob_payload_layout() -> None:
    dv = DeletionVector.from_positions("file.parquet", [1, 2, 3])
    payload = dv.to_blob().payload

    # length (4 bytes, big-endian) | magic (4 bytes) | vector | CRC-32 (4 bytes, big-endian),
    # where the length and the CRC-32 cover the magic and the vector
    length = int.from_bytes(payload[0:4], "big")
    assert payload[4:8] == DELETION_VECTOR_MAGIC == b"\xd1\xd3\x39\x64"
    vector = payload[8 : 4 + length]
    assert vector == dv.serialize()
    assert length == 4 + len(vector)
    assert len(payload) == 4 + length + 4
    assert int.from_bytes(payload[-4:], "big") == zlib.crc32(DELETION_VECTOR_MAGIC + vector)

    # The reader accepts what the writer produces
    assert _deserialize_dv_blob(payload, record_count=3) == dv._bitmaps


def _referenced_data_file(file_path: str, partition: Record, spec_id: int) -> DataFile:
    return DataFile.from_args(
        _table_format_version=3,
        spec_id=spec_id,
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=100,
        file_size_in_bytes=1000,
    )


def test_write_deletion_vectors(tmp_path: Path) -> None:
    io = PyArrowFileIO()
    location = str(tmp_path / "deletes.puffin")
    first = _referenced_data_file("s3://bucket/a.parquet", Record(1), spec_id=1)
    second = _referenced_data_file("s3://bucket/b.parquet", Record(2), spec_id=1)

    delete_files = write_deletion_vectors(
        io,
        location,
        [
            (first, DeletionVector.from_positions(first.file_path, [0, 7])),
            (second, DeletionVector.from_positions(second.file_path, [1, 2, 3])),
        ],
    )

    file_size = Path(location).stat().st_size
    footer_blobs = PuffinFile(Path(location).read_bytes()).footer.blobs
    assert len(delete_files) == 2
    for delete_file, data_file, blob, cardinality in zip(delete_files, [first, second], footer_blobs, [2, 3], strict=True):
        assert delete_file.content == DataFileContent.POSITION_DELETES
        assert delete_file.file_format == FileFormat.PUFFIN
        assert delete_file.file_path == location
        assert delete_file.file_size_in_bytes == file_size
        assert delete_file.partition == data_file.partition
        assert delete_file.spec_id == data_file.spec_id
        assert delete_file.referenced_data_file == data_file.file_path
        assert delete_file.content_offset == blob.offset
        assert delete_file.content_size_in_bytes == blob.length
        assert delete_file.record_count == cardinality
        assert not delete_file.column_sizes
        assert not delete_file.lower_bounds

    assert [dv.to_vector().to_pylist() for delete_file in delete_files for dv in read_deletion_vectors(io, delete_file)] == [
        [0, 7],
        [1, 2, 3],
    ]


def test_write_deletion_vectors_rejects_mismatched_data_file(tmp_path: Path) -> None:
    data_file = _referenced_data_file("s3://bucket/a.parquet", Record(), spec_id=0)
    with pytest.raises(ValueError, match="does not reference s3://bucket/a.parquet"):
        write_deletion_vectors(
            PyArrowFileIO(), str(tmp_path / "deletes.puffin"), [(data_file, DeletionVector.from_positions("other", [1]))]
        )
