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
import io
import math
import struct
import zlib
from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

from pyroaring import BitMap, FrozenBitMap

from pyiceberg.table.puffin import PuffinBlob, PuffinBlobMetadata, PuffinFile, PuffinWriter

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile

EMPTY_BITMAP = FrozenBitMap()
MAX_JAVA_SIGNED = int(math.pow(2, 31)) - 1
# Largest addressable position, mirroring Java's RoaringPositionBitmap.MAX_POSITION
# (toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE)): the high 32 bits hold the bitmap
# key and the low 32 bits the position within that bitmap.
MAX_POSITION = ((MAX_JAVA_SIGNED - 1) << 32) | 0x80000000
PROPERTY_REFERENCED_DATA_FILE = "referenced-data-file"
PROPERTY_CARDINALITY = "cardinality"
DELETION_VECTOR_V1_BLOB_TYPE = "deletion-vector-v1"
DELETION_VECTOR_MAGIC = b"\xd1\xd3\x39\x64"
# Reserved field id of the row position (_pos) metadata column, referenced by
# deletion-vector-v1 blob metadata (Java: MetadataColumns.ROW_POSITION)
ROW_POSITION_FIELD_ID = 2147483645
# Snapshot id and sequence number of a DV blob are inherited from the manifest entry at commit
_INHERITED = -1
_MAX_DELETION_VECTOR_CONTENT_SIZE = 2**31 - 1
_DV_BLOB_LENGTH = struct.Struct(">I")
_DV_BLOB_MAGIC = struct.Struct("<I")
_DV_BLOB_CRC = struct.Struct(">I")
_DV_BLOB_MAGIC_NUMBER = 1681511377
_ROARING_BITMAP_COUNT_SIZE_BYTES = 8
_DV_BLOB_MIN_SIZE_BYTES = _DV_BLOB_LENGTH.size + _DV_BLOB_MAGIC.size + _ROARING_BITMAP_COUNT_SIZE_BYTES + _DV_BLOB_CRC.size


class DeletionVector:
    referenced_data_file: str
    _bitmaps: list[BitMap]

    def __init__(self, referenced_data_file: str, bitmaps: list[BitMap]) -> None:
        self.referenced_data_file = referenced_data_file
        self._bitmaps = bitmaps

    @classmethod
    def from_positions(cls, referenced_data_file: str, positions: Iterable[int]) -> "DeletionVector":
        """Create a deletion vector marking the given row positions of a data file as deleted."""
        bitmaps_by_key: dict[int, BitMap] = {}
        for position in positions:
            if position < 0 or position > MAX_POSITION:
                raise ValueError(f"Invalid position: {position}, must be between 0 and {MAX_POSITION}")
            key = position >> 32
            if (bitmap := bitmaps_by_key.get(key)) is None:
                bitmap = bitmaps_by_key[key] = BitMap()
            bitmap.add(position & 0xFFFFFFFF)

        if not bitmaps_by_key:
            raise ValueError("Deletion vector must contain at least one position")

        # A list indexed by key, padding gaps with the empty bitmap (mirrors _deserialize_bitmap)
        bitmaps: list[BitMap] = [bitmaps_by_key.get(key, EMPTY_BITMAP) for key in range(max(bitmaps_by_key) + 1)]
        return cls(referenced_data_file, bitmaps)

    @property
    def cardinality(self) -> int:
        """Return the number of deleted positions."""
        return sum(len(bitmap) for bitmap in self._bitmaps)

    def union(self, other: "DeletionVector") -> "DeletionVector":
        """Return a deletion vector with the positions deleted by either vector."""
        if other.referenced_data_file != self.referenced_data_file:
            raise ValueError(f"Cannot union deletion vectors of {self.referenced_data_file} and {other.referenced_data_file}")
        bitmaps: list[BitMap] = []
        for key in range(max(len(self._bitmaps), len(other._bitmaps))):
            left = self._bitmaps[key] if key < len(self._bitmaps) else EMPTY_BITMAP
            right = other._bitmaps[key] if key < len(other._bitmaps) else EMPTY_BITMAP
            bitmaps.append(BitMap(left) | right)
        return DeletionVector(self.referenced_data_file, bitmaps)

    def serialize(self) -> bytes:
        """Serialize the positions as a portable 64-bit Roaring bitmap (the vector inside a deletion-vector-v1 blob)."""
        return self._serialize_bitmap(self._bitmaps)

    def to_blob(self) -> PuffinBlob:
        """Return the deletion-vector-v1 Puffin blob of this vector; offset and length are assigned by the writer."""
        metadata = PuffinBlobMetadata(
            type=DELETION_VECTOR_V1_BLOB_TYPE,
            fields=[ROW_POSITION_FIELD_ID],
            snapshot_id=_INHERITED,
            sequence_number=_INHERITED,
            offset=0,
            length=0,
            properties={
                PROPERTY_REFERENCED_DATA_FILE: self.referenced_data_file,
                PROPERTY_CARDINALITY: str(self.cardinality),
            },
        )
        return PuffinBlob(metadata=metadata, payload=_serialize_dv_blob(self.serialize()))

    @staticmethod
    def _deserialize_bitmap(pl: bytes) -> list[BitMap]:
        number_of_bitmaps = int.from_bytes(pl[0:8], byteorder="little")
        pl = pl[8:]

        # Every bitmap contributes at least a 4-byte key, so a count that cannot fit
        # in the remaining payload is invalid and must not be used as a loop bound.
        if number_of_bitmaps * 4 > len(pl):
            raise ValueError(f"Payload declares {number_of_bitmaps} bitmaps, but only holds {len(pl)} bytes")

        bitmaps = []
        last_key = -1
        for _ in range(number_of_bitmaps):
            key = int.from_bytes(pl[0:4], byteorder="little")
            if key < 0:
                raise ValueError(f"Invalid unsigned key: {key}")
            if key <= last_key:
                raise ValueError("Keys must be sorted in ascending order")
            if key > MAX_JAVA_SIGNED:
                raise ValueError(f"Key {key} is too large, max {MAX_JAVA_SIGNED} to maintain compatibility with Java impl")
            pl = pl[4:]

            while last_key < key - 1:
                bitmaps.append(EMPTY_BITMAP)
                last_key += 1

            bm = BitMap().deserialize(pl)
            # TODO: Optimize this
            pl = pl[len(bm.serialize()) :]
            bitmaps.append(bm)

            last_key = key

        return bitmaps

    @staticmethod
    def _serialize_bitmap(bitmaps: list[BitMap]) -> bytes:
        # Counterpart of _deserialize_bitmap: number of bitmaps (8 bytes, little-endian), then for each
        # non-empty bitmap in ascending key order its key (4 bytes, little-endian) and portable payload.
        non_empty = [(key, bitmap) for key, bitmap in enumerate(bitmaps) if len(bitmap) > 0]

        with io.BytesIO() as out:
            out.write(len(non_empty).to_bytes(8, byteorder="little"))
            for key, bitmap in non_empty:
                # Java's RoaringPositionBitmap rejects keys above Integer.MAX_VALUE - 1
                if key > MAX_JAVA_SIGNED - 1:
                    raise ValueError(f"Key {key} is too large, max {MAX_JAVA_SIGNED - 1} for compatibility with the Java impl")
                out.write(key.to_bytes(4, byteorder="little"))
                # Run-length encode so contiguous deletes stay compact, matching Java's BitmapPositionDeleteIndex
                optimized = BitMap(bitmap)
                optimized.run_optimize()
                out.write(optimized.serialize())
            return out.getvalue()

    @staticmethod
    def _bitmaps_to_chunked_array(bitmaps: list[BitMap]) -> "pa.ChunkedArray":
        import pyarrow as pa

        return pa.chunked_array([(key_pos << 32) + pos for pos in bitmap] for key_pos, bitmap in enumerate(bitmaps))

    def to_vector(self) -> "pa.ChunkedArray":
        return self._bitmaps_to_chunked_array(self._bitmaps)


def _serialize_dv_blob(bitmap_payload: bytes) -> bytes:
    # Counterpart of _deserialize_dv_blob: 4-byte big-endian length of magic + vector, the magic,
    # the vector, and a 4-byte big-endian CRC-32 of magic + vector.
    bitmap_data = _DV_BLOB_MAGIC.pack(_DV_BLOB_MAGIC_NUMBER) + bitmap_payload
    if len(bitmap_data) > _MAX_DELETION_VECTOR_CONTENT_SIZE:
        raise ValueError(f"Cannot write deletion vector larger than 2GB: {len(bitmap_data)}")
    return _DV_BLOB_LENGTH.pack(len(bitmap_data)) + bitmap_data + _DV_BLOB_CRC.pack(zlib.crc32(bitmap_data) & 0xFFFFFFFF)


def _deserialize_dv_blob(blob: bytes, record_count: int | None = None) -> list[BitMap]:
    # The DV blob encoding matches Iceberg Java's BitmapPositionDeleteIndex:
    # 4-byte big-endian bitmap-data length, 4-byte little-endian magic number,
    # portable Roaring bitmap data, and 4-byte big-endian CRC-32.
    if len(blob) < _DV_BLOB_MIN_SIZE_BYTES:
        raise ValueError(f"Invalid deletion vector blob length: {len(blob)}")

    bitmap_data_length = _DV_BLOB_LENGTH.unpack_from(blob)[0]
    expected_bitmap_data_length = len(blob) - _DV_BLOB_LENGTH.size - _DV_BLOB_CRC.size
    if bitmap_data_length != expected_bitmap_data_length:
        raise ValueError(f"Invalid bitmap data length: {bitmap_data_length}, expected {expected_bitmap_data_length}")

    bitmap_data_offset = _DV_BLOB_LENGTH.size
    crc_offset = bitmap_data_offset + bitmap_data_length
    bitmap_data = blob[bitmap_data_offset:crc_offset]

    magic_number = _DV_BLOB_MAGIC.unpack_from(bitmap_data)[0]
    if magic_number != _DV_BLOB_MAGIC_NUMBER:
        raise ValueError(f"Invalid magic number: {magic_number}, expected {_DV_BLOB_MAGIC_NUMBER}")

    checksum = zlib.crc32(bitmap_data) & 0xFFFFFFFF
    expected_checksum = _DV_BLOB_CRC.unpack_from(blob, crc_offset)[0]
    if checksum != expected_checksum:
        raise ValueError("Invalid CRC")

    bitmaps = DeletionVector._deserialize_bitmap(bitmap_data[_DV_BLOB_MAGIC.size :])
    if record_count is not None:
        cardinality = sum(len(bitmap) for bitmap in bitmaps)
        if cardinality != record_count:
            raise ValueError(f"Invalid cardinality: {cardinality}, expected {record_count}")

    return bitmaps


def _validate_deletion_vector_content(dv: "DataFile") -> None:
    content_offset = dv.content_offset
    content_size_in_bytes = dv.content_size_in_bytes
    referenced_data_file = dv.referenced_data_file

    if content_offset is None:
        raise ValueError(f"Invalid deletion vector, content offset is missing: {dv.file_path}")
    if content_size_in_bytes is None:
        raise ValueError(f"Invalid deletion vector, content size is missing: {dv.file_path}")
    if content_offset < 0:
        raise ValueError(f"Invalid deletion vector, content offset cannot be negative: {content_offset}")
    if content_size_in_bytes < 0:
        raise ValueError(f"Invalid deletion vector, content size cannot be negative: {content_size_in_bytes}")
    if content_size_in_bytes > _MAX_DELETION_VECTOR_CONTENT_SIZE:
        raise ValueError(f"Cannot read deletion vector larger than 2GB: {content_size_in_bytes}")
    if referenced_data_file is None:
        raise ValueError(f"Invalid deletion vector, referenced data file is missing: {dv.file_path}")


def has_deletion_vector_content_reference(dv: "DataFile") -> bool:
    """Return whether a deletion vector is described by manifest content-range metadata."""
    return dv.content_offset is not None or dv.content_size_in_bytes is not None or dv.referenced_data_file is not None


def _read_deletion_vector(io: "FileIO", dv: "DataFile") -> DeletionVector:
    _validate_deletion_vector_content(dv)

    content_offset = cast(int, dv.content_offset)
    content_size_in_bytes = cast(int, dv.content_size_in_bytes)
    referenced_data_file = cast(str, dv.referenced_data_file)

    with io.new_input(dv.file_path).open() as fi:
        fi.seek(content_offset)
        payload = fi.read(content_size_in_bytes)

    if len(payload) != content_size_in_bytes:
        raise ValueError(f"Could not read deletion vector, expected {content_size_in_bytes} bytes, got {len(payload)}")

    return DeletionVector(
        referenced_data_file=referenced_data_file,
        bitmaps=_deserialize_dv_blob(payload, dv.record_count),
    )


def read_deletion_vectors(io: "FileIO", dv: "DataFile") -> list[DeletionVector]:
    """Read deletion vectors from a delete file or its manifest content range."""
    if has_deletion_vector_content_reference(dv):
        return [_read_deletion_vector(io, dv)]

    with io.new_input(dv.file_path).open() as fi:
        return deletion_vectors_from_puffin_file(PuffinFile(fi.read()))


def deletion_vectors_from_puffin_file(puffin_file: PuffinFile) -> list[DeletionVector]:
    """Read all deletion vectors stored in a Puffin file, skipping blobs of other types."""
    deletion_vectors = []
    for blob in puffin_file.footer.blobs:
        if blob.type != DELETION_VECTOR_V1_BLOB_TYPE:
            continue
        if PROPERTY_REFERENCED_DATA_FILE not in blob.properties:
            raise ValueError(f"Invalid deletion vector blob at offset {blob.offset}, {PROPERTY_REFERENCED_DATA_FILE} is missing")
        deletion_vectors.append(
            DeletionVector(
                referenced_data_file=blob.properties[PROPERTY_REFERENCED_DATA_FILE],
                bitmaps=_deserialize_dv_blob(puffin_file.get_blob_payload(blob)),
            )
        )
    return deletion_vectors


def write_deletion_vectors(
    io: "FileIO", location: str, deletion_vectors: Iterable[tuple["DataFile", DeletionVector]]
) -> list["DataFile"]:
    """Write deletion vectors into one Puffin file and return a delete file entry per vector.

    Args:
        io: The FileIO to write the Puffin file with.
        location: The location of the Puffin file.
        deletion_vectors: Pairs of the referenced data file and its deletion vector. The data file
            provides the partition and partition spec of the returned delete file.

    Returns:
        The position delete files, one per deletion vector, carrying the blob's content range.
    """
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

    written: list[tuple[DataFile, DeletionVector, PuffinBlobMetadata]] = []
    with PuffinWriter(io.new_output(location)) as writer:
        for data_file, dv in deletion_vectors:
            if dv.referenced_data_file != data_file.file_path:
                raise ValueError(f"Deletion vector for {dv.referenced_data_file} does not reference {data_file.file_path}")
            written.append((data_file, dv, writer.add_blob(dv.to_blob())))

    file_size = cast(int, writer.file_size)
    return [
        DataFile.from_args(
            _table_format_version=3,
            spec_id=data_file.spec_id,
            content=DataFileContent.POSITION_DELETES,
            file_path=location,
            file_format=FileFormat.PUFFIN,
            partition=data_file.partition,
            record_count=dv.cardinality,
            file_size_in_bytes=file_size,
            referenced_data_file=dv.referenced_data_file,
            content_offset=blob.offset,
            content_size_in_bytes=blob.length,
        )
        for data_file, dv, blob in written
    ]
