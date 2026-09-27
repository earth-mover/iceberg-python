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
r"""Decoder for the Variant binary encoding.

Iceberg stores variant values in the Variant binary encoding defined by the Parquet project
(https://github.com/apache/parquet-format/blob/master/VariantEncoding.md): a ``metadata`` binary holding
the dictionary of object keys, and a ``value`` binary holding the encoded value. Scans return variant
columns as a struct of these two binaries; the functions in this module decode one value.

Example:
    >>> from pyiceberg.variant import to_json, to_python
    >>> to_python(b"\\x01\\x00\\x00", b"\\x35just a string")
    'just a string'
    >>> to_json(b"\\x01\\x01\\x00\\x01a", b"\\x02\\x01\\x00\\x00\\x02\\x0c\\x01")
    '{"a":1}'
"""

from __future__ import annotations

import base64
import json
import math
import struct
import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from pyiceberg.utils.datetime import EPOCH_DATE, EPOCH_TIMESTAMP, EPOCH_TIMESTAMPTZ
from pyiceberg.utils.decimal import unscaled_to_decimal

VARIANT_VERSION = 1

# Basic types, stored in the lower two bits of the first byte of a value
_PRIMITIVE = 0
_SHORT_STRING = 1
_OBJECT = 2

# Primitive type ids, stored in the upper six bits of the first byte of a primitive value
_NULL = 0
_TRUE = 1
_FALSE = 2
_INT8 = 3
_INT16 = 4
_INT32 = 5
_INT64 = 6
_DOUBLE = 7
_DECIMAL4 = 8
_DECIMAL8 = 9
_DECIMAL16 = 10
_DATE = 11
_TIMESTAMPTZ = 12
_TIMESTAMP = 13
_FLOAT = 14
_BINARY = 15
_STRING = 16
_TIME = 17
_TIMESTAMPTZ_NANOS = 18
_TIMESTAMP_NANOS = 19
_UUID = 20

_FIXED_WIDTH_FORMATS = {
    _INT8: "<b",
    _INT16: "<h",
    _INT32: "<i",
    _INT64: "<q",
    _DOUBLE: "<d",
    _FLOAT: "<f",
}

_DECIMAL_WIDTHS = {_DECIMAL4: 4, _DECIMAL8: 8, _DECIMAL16: 16}


class _NanoTimestamp:
    """A timestamp with nanosecond precision, which ``datetime`` cannot hold."""

    __slots__ = ("nanos", "utc")

    def __init__(self, nanos: int, utc: bool) -> None:
        self.nanos = nanos
        self.utc = utc

    def to_datetime(self) -> datetime:
        """Return the timestamp truncated to microseconds."""
        return (EPOCH_TIMESTAMPTZ if self.utc else EPOCH_TIMESTAMP) + timedelta(microseconds=self.nanos // 1000)

    def isoformat(self) -> str:
        """Return the ISO-8601 representation with nanosecond precision."""
        seconds = self.to_datetime().replace(microsecond=0).isoformat()
        fraction = f".{self.nanos % 1_000_000_000:09d}"
        if self.utc:
            # Place the fraction before the "+00:00" offset
            return f"{seconds[:-6]}{fraction}{seconds[-6:]}"
        return f"{seconds}{fraction}"


def _read_unsigned(buf: bytes, pos: int, size: int) -> int:
    if pos + size > len(buf):
        raise ValueError("Invalid variant: unexpected end of buffer")
    return int.from_bytes(buf[pos : pos + size], "little", signed=False)


def _parse_metadata(metadata: bytes) -> list[str]:
    """Return the dictionary of object keys stored in variant metadata."""
    if len(metadata) == 0:
        raise ValueError("Invalid variant metadata: empty buffer")
    header = metadata[0]
    version = header & 0x0F
    if version != VARIANT_VERSION:
        raise ValueError(f"Unsupported variant metadata version: {version}")
    offset_size = ((header >> 6) & 0x03) + 1
    dictionary_size = _read_unsigned(metadata, 1, offset_size)
    offsets_start = 1 + offset_size
    offsets = [_read_unsigned(metadata, offsets_start + index * offset_size, offset_size) for index in range(dictionary_size + 1)]
    strings_start = offsets_start + (dictionary_size + 1) * offset_size
    if strings_start + offsets[-1] > len(metadata):
        raise ValueError("Invalid variant metadata: unexpected end of buffer")
    return [
        metadata[strings_start + offsets[index] : strings_start + offsets[index + 1]].decode("utf-8")
        for index in range(dictionary_size)
    ]


def _decode_primitive(type_id: int, value: bytes, pos: int) -> Any:
    if type_id == _NULL:
        return None
    if type_id == _TRUE:
        return True
    if type_id == _FALSE:
        return False
    if (fmt := _FIXED_WIDTH_FORMATS.get(type_id)) is not None:
        width = struct.calcsize(fmt)
        if pos + width > len(value):
            raise ValueError("Invalid variant: unexpected end of buffer")
        return struct.unpack_from(fmt, value, pos)[0]
    if (decimal_width := _DECIMAL_WIDTHS.get(type_id)) is not None:
        scale = _read_unsigned(value, pos, 1)
        unscaled = int.from_bytes(value[pos + 1 : pos + 1 + decimal_width], "little", signed=True)
        return unscaled_to_decimal(unscaled, scale)
    if type_id == _DATE:
        return EPOCH_DATE + timedelta(days=struct.unpack_from("<i", value, pos)[0])
    if type_id == _TIMESTAMPTZ:
        return EPOCH_TIMESTAMPTZ + timedelta(microseconds=struct.unpack_from("<q", value, pos)[0])
    if type_id == _TIMESTAMP:
        return EPOCH_TIMESTAMP + timedelta(microseconds=struct.unpack_from("<q", value, pos)[0])
    if type_id == _TIMESTAMPTZ_NANOS:
        return _NanoTimestamp(struct.unpack_from("<q", value, pos)[0], utc=True)
    if type_id == _TIMESTAMP_NANOS:
        return _NanoTimestamp(struct.unpack_from("<q", value, pos)[0], utc=False)
    if type_id == _TIME:
        micros = struct.unpack_from("<q", value, pos)[0]
        return (datetime.min + timedelta(microseconds=micros)).time()
    if type_id in (_BINARY, _STRING):
        length = _read_unsigned(value, pos, 4)
        data = value[pos + 4 : pos + 4 + length]
        if len(data) != length:
            raise ValueError("Invalid variant: unexpected end of buffer")
        return data.decode("utf-8") if type_id == _STRING else bytes(data)
    if type_id == _UUID:
        return uuid.UUID(bytes=bytes(value[pos : pos + 16]))
    raise ValueError(f"Unsupported variant primitive type id: {type_id}")


def _decode(keys: list[str], value: bytes, pos: int) -> Any:
    if pos >= len(value):
        raise ValueError("Invalid variant: unexpected end of buffer")
    value_metadata = value[pos]
    basic_type = value_metadata & 0x03
    header = value_metadata >> 2

    if basic_type == _PRIMITIVE:
        return _decode_primitive(header, value, pos + 1)

    if basic_type == _SHORT_STRING:
        data = value[pos + 1 : pos + 1 + header]
        if len(data) != header:
            raise ValueError("Invalid variant: unexpected end of buffer")
        return data.decode("utf-8")

    if basic_type == _OBJECT:
        offset_size = (header & 0x03) + 1
        field_id_size = ((header >> 2) & 0x03) + 1
        is_large = (header >> 4) & 0x01
        num_elements_size = 4 if is_large else 1
        num_elements = _read_unsigned(value, pos + 1, num_elements_size)
        field_ids_start = pos + 1 + num_elements_size
        offsets_start = field_ids_start + num_elements * field_id_size
        values_start = offsets_start + (num_elements + 1) * offset_size
        result: dict[str, Any] = {}
        for index in range(num_elements):
            field_id = _read_unsigned(value, field_ids_start + index * field_id_size, field_id_size)
            if field_id >= len(keys):
                raise ValueError(f"Invalid variant: field id {field_id} is not in the metadata dictionary")
            offset = _read_unsigned(value, offsets_start + index * offset_size, offset_size)
            result[keys[field_id]] = _decode(keys, value, values_start + offset)
        return result

    # The remaining basic type is an array
    offset_size = (header & 0x03) + 1
    is_large = (header >> 2) & 0x01
    num_elements_size = 4 if is_large else 1
    num_elements = _read_unsigned(value, pos + 1, num_elements_size)
    offsets_start = pos + 1 + num_elements_size
    values_start = offsets_start + (num_elements + 1) * offset_size
    return [
        _decode(keys, value, values_start + _read_unsigned(value, offsets_start + index * offset_size, offset_size))
        for index in range(num_elements)
    ]


def _convert(decoded: Any, nano_timestamp: Callable[[_NanoTimestamp], Any]) -> Any:
    if isinstance(decoded, _NanoTimestamp):
        return nano_timestamp(decoded)
    if isinstance(decoded, dict):
        return {key: _convert(item, nano_timestamp) for key, item in decoded.items()}
    if isinstance(decoded, list):
        return [_convert(item, nano_timestamp) for item in decoded]
    return decoded


def to_python(metadata: bytes, value: bytes) -> Any:
    """Decode a variant value into Python objects.

    Objects decode to ``dict``, arrays to ``list``, and primitives to ``None``, ``bool``, ``int``, ``float``,
    ``Decimal``, ``str``, ``bytes``, ``date``, ``time``, ``datetime`` (timezone-aware in UTC for timestamps
    with a time zone) or ``UUID``. Nanosecond timestamps are truncated to microseconds, the precision
    of ``datetime``; use ``to_json`` to keep all digits.

    Args:
        metadata: The variant metadata, holding the dictionary of object keys.
        value: The encoded variant value.

    Returns:
        The decoded value.

    Raises:
        ValueError: If the buffers are not a valid variant.
    """
    decoded = _decode(_parse_metadata(bytes(metadata)), bytes(value), 0)
    return _convert(decoded, _NanoTimestamp.to_datetime)


def _json_text(decoded: Any) -> str:
    if decoded is None:
        return "null"
    if isinstance(decoded, bool):
        return "true" if decoded else "false"
    if isinstance(decoded, int):
        return str(decoded)
    if isinstance(decoded, float):
        # JSON has no representation for NaN and infinity
        return json.dumps(str(decoded)) if math.isnan(decoded) or math.isinf(decoded) else repr(decoded)
    if isinstance(decoded, Decimal):
        return format(decoded, "f")
    if isinstance(decoded, str):
        return json.dumps(decoded, ensure_ascii=False)
    if isinstance(decoded, bytes):
        return json.dumps(base64.b64encode(decoded).decode("ascii"))
    if isinstance(decoded, (date, time, datetime, _NanoTimestamp)):
        return json.dumps(decoded.isoformat())
    if isinstance(decoded, uuid.UUID):
        return json.dumps(str(decoded))
    if isinstance(decoded, dict):
        return "{" + ",".join(f"{json.dumps(key, ensure_ascii=False)}:{_json_text(item)}" for key, item in decoded.items()) + "}"
    if isinstance(decoded, list):
        return "[" + ",".join(_json_text(item) for item in decoded) + "]"
    raise ValueError(f"Cannot convert {decoded!r} to JSON")


def to_json(metadata: bytes, value: bytes) -> str:
    """Decode a variant value into a JSON string.

    Decimals are written as JSON numbers with all their digits; binary values as base64 strings; dates,
    times and timestamps as ISO-8601 strings; UUIDs as strings; NaN and infinite floats as strings.

    Args:
        metadata: The variant metadata, holding the dictionary of object keys.
        value: The encoded variant value.

    Returns:
        The JSON text of the value.

    Raises:
        ValueError: If the buffers are not a valid variant.
    """
    return _json_text(_decode(_parse_metadata(bytes(metadata)), bytes(value), 0))


__all__ = ["to_json", "to_python", "VARIANT_VERSION"]
