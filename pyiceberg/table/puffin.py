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
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING

import zstandard
from pydantic import Field

from pyiceberg import __version__
from pyiceberg.io import OutputFile
from pyiceberg.typedef import IcebergBaseModel
from pyiceberg.utils.deprecated import deprecated

if TYPE_CHECKING:
    import pyarrow as pa

# Short for: Puffin Fratercula arctica, version 1
MAGIC_BYTES = b"PFA1"
# Footer trailer: footer payload size (4 bytes), flags (4 bytes) and the closing magic
_FOOTER_TRAILER_SIZE = 12


class PuffinBlobMetadata(IcebergBaseModel):
    type: str = Field()
    fields: list[int] = Field()
    snapshot_id: int = Field(alias="snapshot-id")
    sequence_number: int = Field(alias="sequence-number")
    offset: int = Field()
    length: int = Field()
    compression_codec: str | None = Field(alias="compression-codec", default=None)
    properties: dict[str, str] = Field(default_factory=dict)


class Footer(IcebergBaseModel):
    blobs: list[PuffinBlobMetadata] = Field()
    properties: dict[str, str] = Field(default_factory=dict)


class PuffinFile:
    footer: Footer
    _file_bytes: bytes

    def __init__(self, puffin: bytes) -> None:
        for magic_bytes in [puffin[:4], puffin[-4:]]:
            if magic_bytes != MAGIC_BYTES:
                raise ValueError(f"Incorrect magic bytes, expected {MAGIC_BYTES!r}, got {magic_bytes!r}")

        # One flag is set, the rest should be zero
        # byte 0 (first)
        # - bit 0 (lowest bit): whether FooterPayload is compressed
        # - all other bits are reserved for future use and should be set to 0 on write
        flags = puffin[-8:-4]
        if flags[0] != 0:
            raise ValueError("The Puffin-file has a compressed footer, which is not yet supported")

        # 4 byte integer is always signed, in a two's complement representation, stored little-endian.
        footer_payload_size_int = int.from_bytes(puffin[-12:-8], byteorder="little")

        self.footer = Footer.model_validate_json(puffin[-(footer_payload_size_int + 12) : -12])
        self._file_bytes = puffin

    def get_blob_payload(self, blob: PuffinBlobMetadata) -> bytes:
        raw = self._file_bytes[blob.offset : blob.offset + blob.length]
        if blob.compression_codec is None:
            return raw
        elif blob.compression_codec == "zstd":
            return zstandard.ZstdDecompressor().decompress(raw)
        else:
            raise ValueError(f"Unsupported puffin compression codec: {blob.compression_codec!r}")

    @deprecated(deprecated_in="0.12.0", removed_in="0.13.0", help_message="Use deletion_vectors_from_puffin_file(...) instead")
    def to_vector(self) -> dict[str, "pa.ChunkedArray"]:
        from pyiceberg.table.deletion_vector import deletion_vectors_from_puffin_file  # local import avoids the cycle

        return {dv.referenced_data_file: dv.to_vector() for dv in deletion_vectors_from_puffin_file(self)}


@dataclass(frozen=True)
class PuffinBlob:
    """A blob to write into a Puffin file: its metadata and serialized payload."""

    metadata: PuffinBlobMetadata
    payload: bytes


class PuffinWriter:
    """Assembles a Puffin file from blobs and writes it to an output file.

    The writer is blob-agnostic: callers supply already-serialized blobs (for example via
    ``DeletionVector.to_blob()``). The offset and length of each blob are assigned when it is
    added, and the file is written on ``close()`` (or on exiting the context manager without
    an exception), after which ``file_size`` holds the size of the written file.
    """

    closed: bool
    file_size: int | None
    _output_file: OutputFile
    _blobs: list[PuffinBlob]
    _next_offset: int
    _created_by: str

    def __init__(self, output_file: OutputFile, created_by: str | None = None) -> None:
        self.closed = False
        self.file_size = None
        self._output_file = output_file
        self._blobs = []
        self._next_offset = len(MAGIC_BYTES)
        self._created_by = created_by if created_by is not None else f"PyIceberg version {__version__}"

    def __enter__(self) -> "PuffinWriter":
        """Open the writer."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Write the Puffin file, unless the body raised."""
        if exc_type is not None:
            # Skip writing a half-populated file
            self.closed = True
            return
        self.close()

    @property
    def location(self) -> str:
        """Return the location of the output file."""
        return self._output_file.location

    def add_blob(self, blob: PuffinBlob) -> PuffinBlobMetadata:
        """Add a blob and return its metadata with the offset and length it has in the file."""
        if self.closed:
            raise RuntimeError("Cannot add blob to closed Puffin writer")
        metadata = blob.metadata.model_copy(update={"offset": self._next_offset, "length": len(blob.payload)})
        self._blobs.append(PuffinBlob(metadata=metadata, payload=blob.payload))
        self._next_offset += len(blob.payload)
        return metadata

    def close(self) -> int:
        """Write the blobs and the footer to the output file and return the size of the file."""
        if self.closed:
            raise RuntimeError("Puffin writer is already closed")
        self.closed = True

        footer = Footer(blobs=[blob.metadata for blob in self._blobs], properties={"created-by": self._created_by})
        footer_payload = footer.model_dump_json(by_alias=True, exclude_none=True).encode("utf-8")

        with self._output_file.create(overwrite=True) as output_stream:
            output_stream.write(MAGIC_BYTES)
            for blob in self._blobs:
                output_stream.write(blob.payload)
            output_stream.write(MAGIC_BYTES)
            output_stream.write(footer_payload)
            output_stream.write(len(footer_payload).to_bytes(4, byteorder="little"))
            # Flags: the footer payload is not compressed
            output_stream.write((0).to_bytes(4, byteorder="little"))
            output_stream.write(MAGIC_BYTES)

        self.file_size = self._next_offset + len(MAGIC_BYTES) + len(footer_payload) + _FOOTER_TRAILER_SIZE
        return self.file_size
