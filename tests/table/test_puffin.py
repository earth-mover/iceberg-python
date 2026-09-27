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
import json
import struct
import zlib
from os import path
from pathlib import Path

import pyarrow as pa
from pyroaring import BitMap

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.deletion_vector import (
    _DV_BLOB_MAGIC_NUMBER,
    DELETION_VECTOR_V1_BLOB_TYPE,
    PROPERTY_REFERENCED_DATA_FILE,
    deletion_vectors_from_puffin_file,
    read_deletion_vectors,
)
from pyiceberg.table.puffin import MAGIC_BYTES, PuffinFile


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/puffin/{file}", "rb") as f:
        return f.read()


def test_read_empty_uncompressed() -> None:
    puffin_bytes = _open_file("v1/empty-puffin-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.blobs == []
    assert pf.footer.properties == {}


def test_read_compressed_zstd() -> None:
    puffin_bytes = _open_file("v1/sample-metric-data-compressed-zstd.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.properties == {"created-by": "Test 1234"}
    assert len(pf.footer.blobs) == 2

    blob1 = pf.footer.blobs[0]
    assert blob1.type == "some-blob"
    assert blob1.fields == [1]
    assert blob1.snapshot_id == 2
    assert blob1.sequence_number == 1
    assert blob1.compression_codec == "zstd"
    assert pf.get_blob_payload(blob1) == b"abcdefghi"

    blob2 = pf.footer.blobs[1]
    assert blob2.type == "some-other-blob"
    assert blob2.fields == [2]
    assert blob2.compression_codec == "zstd"
    assert pf.get_blob_payload(blob2) == (
        b"some blob \x00 binary data \xf0\x9f\xa4\xaf that is not very very very very very very long, is it?"
    )


def test_read_two_blobs_uncompressed() -> None:
    puffin_bytes = _open_file("v1/sample-metric-data-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.properties == {"created-by": "Test 1234"}
    assert len(pf.footer.blobs) == 2

    blob1 = pf.footer.blobs[0]
    assert blob1.type == "some-blob"
    assert blob1.fields == [1]
    assert blob1.snapshot_id == 2
    assert blob1.sequence_number == 1
    assert blob1.compression_codec is None
    assert pf.get_blob_payload(blob1) == b"abcdefghi"

    blob2 = pf.footer.blobs[1]
    assert blob2.type == "some-other-blob"
    assert blob2.fields == [2]
    assert blob2.compression_codec is None
    assert pf.get_blob_payload(blob2) == (
        b"some blob \x00 binary data \xf0\x9f\xa4\xaf that is not very very very very very very long, is it?"
    )


def _dv_blob(positions: list[int]) -> bytes:
    bitmap_data = (
        struct.pack("<I", _DV_BLOB_MAGIC_NUMBER)
        + (1).to_bytes(8, byteorder="little")
        + (0).to_bytes(4, byteorder="little")
        + BitMap(positions).serialize()
    )
    return struct.pack(">I", len(bitmap_data)) + bitmap_data + struct.pack(">I", zlib.crc32(bitmap_data) & 0xFFFFFFFF)


def _puffin_with_blobs(blobs: list[tuple[str, str, bytes]]) -> tuple[bytes, list[tuple[int, int]]]:
    """Build a Puffin file from (type, referenced data file, payload) tuples, returning the (offset, length) of each blob."""
    body = MAGIC_BYTES
    ranges = []
    blob_metadata = []
    for blob_type, referenced_data_file, payload in blobs:
        ranges.append((len(body), len(payload)))
        blob_metadata.append(
            {
                "type": blob_type,
                "fields": [2147483545],
                "snapshot-id": 1,
                "sequence-number": 1,
                "offset": len(body),
                "length": len(payload),
                "properties": {PROPERTY_REFERENCED_DATA_FILE: referenced_data_file},
            }
        )
        body += payload
    footer_payload = json.dumps({"blobs": blob_metadata, "properties": {}}).encode()
    footer = (
        MAGIC_BYTES + footer_payload + len(footer_payload).to_bytes(4, byteorder="little") + b"\x00\x00\x00\x00" + MAGIC_BYTES
    )
    return body + footer, ranges


def _dv_data_file(delete_file_path: str, referenced_data_file: str, offset: int, length: int, cardinality: int) -> DataFile:
    return DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_file_path,
        file_format=FileFormat.PUFFIN,
        record_count=cardinality,
        referenced_data_file=referenced_data_file,
        content_offset=offset,
        content_size_in_bytes=length,
    )


def test_read_two_deletion_vectors_by_range(tmp_path: Path) -> None:
    puffin_bytes, ranges = _puffin_with_blobs(
        [
            (DELETION_VECTOR_V1_BLOB_TYPE, "s3://bucket/a.parquet", _dv_blob([1, 3])),
            (DELETION_VECTOR_V1_BLOB_TYPE, "s3://bucket/b.parquet", _dv_blob([0, 2, 4])),
        ]
    )
    delete_file_path = str(tmp_path / "deletes.puffin")
    with open(delete_file_path, "wb") as f:
        f.write(puffin_bytes)

    io = PyArrowFileIO()
    (first,) = read_deletion_vectors(io, _dv_data_file(delete_file_path, "s3://bucket/a.parquet", *ranges[0], cardinality=2))
    (second,) = read_deletion_vectors(io, _dv_data_file(delete_file_path, "s3://bucket/b.parquet", *ranges[1], cardinality=3))

    assert first.referenced_data_file == "s3://bucket/a.parquet"
    assert first.to_vector() == pa.chunked_array([[1, 3]])
    assert second.referenced_data_file == "s3://bucket/b.parquet"
    assert second.to_vector() == pa.chunked_array([[0, 2, 4]])

    # Reading the whole file yields both vectors
    assert {dv.referenced_data_file: dv.to_vector() for dv in deletion_vectors_from_puffin_file(PuffinFile(puffin_bytes))} == {
        "s3://bucket/a.parquet": pa.chunked_array([[1, 3]]),
        "s3://bucket/b.parquet": pa.chunked_array([[0, 2, 4]]),
    }


def test_deletion_vectors_from_puffin_file_skips_other_blob_types() -> None:
    puffin_bytes, _ = _puffin_with_blobs(
        [
            ("apache-datasketches-theta-v1", "s3://bucket/a.parquet", b"not-a-deletion-vector"),
            (DELETION_VECTOR_V1_BLOB_TYPE, "s3://bucket/b.parquet", _dv_blob([7])),
        ]
    )

    deletion_vectors = deletion_vectors_from_puffin_file(PuffinFile(puffin_bytes))

    assert [dv.referenced_data_file for dv in deletion_vectors] == ["s3://bucket/b.parquet"]
