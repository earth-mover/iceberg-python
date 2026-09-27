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
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

import pytest

from pyiceberg.variant import to_json, to_python

EMPTY_METADATA = b"\x01\x00\x00"


def _metadata(*keys: str) -> bytes:
    """Encode a metadata dictionary with 1-byte offsets."""
    encoded = [key.encode() for key in keys]
    offsets = [0]
    for key in encoded:
        offsets.append(offsets[-1] + len(key))
    return bytes([0x01, len(keys), *offsets]) + b"".join(encoded)


def _primitive(type_id: int, payload: bytes = b"") -> bytes:
    return bytes([type_id << 2]) + payload


def _short_string(value: str) -> bytes:
    data = value.encode()
    return bytes([(len(data) << 2) | 1]) + data


def _array(*elements: bytes) -> bytes:
    offsets = [0]
    for element in elements:
        offsets.append(offsets[-1] + len(element))
    return bytes([0x03, len(elements), *offsets]) + b"".join(elements)


def _object(fields: list[tuple[int, bytes]]) -> bytes:
    offsets = []
    position = 0
    for _, element in fields:
        offsets.append(position)
        position += len(element)
    offsets.append(position)
    return bytes([0x02, len(fields), *[field_id for field_id, _ in fields], *offsets]) + b"".join(
        element for _, element in fields
    )


# Written by Spark 4 / Iceberg 1.11 for parse_json('...')
SPARK_SAMPLES = [
    (b"\x01\x01\x00\x01a", b"\x02\x01\x00\x00\x02\x0c\x01", {"a": 1}, '{"a":1}'),
    (
        EMPTY_METADATA,
        b"\x03\x04\x00\x02\x06\x07\r\x0c\x01\rtwo\x00 \x01#\x00\x00\x00",
        [1, "two", None, Decimal("3.5")],
        '[1,"two",null,3.5]',
    ),
    (EMPTY_METADATA, b"\x35just a string", "just a string", '"just a string"'),
    (EMPTY_METADATA, b"\x00", None, "null"),
]


@pytest.mark.parametrize("metadata,value,expected_python,expected_json", SPARK_SAMPLES)
def test_decode_spark_written_variants(metadata: bytes, value: bytes, expected_python: Any, expected_json: str) -> None:
    assert to_python(metadata, value) == expected_python
    assert to_json(metadata, value) == expected_json


@pytest.mark.parametrize(
    "value,expected",
    [
        (_primitive(0), None),
        (_primitive(1), True),
        (_primitive(2), False),
        (_primitive(3, struct.pack("<b", -5)), -5),
        (_primitive(4, struct.pack("<h", -300)), -300),
        (_primitive(5, struct.pack("<i", 70000)), 70000),
        (_primitive(6, struct.pack("<q", -(2**40))), -(2**40)),
        (_primitive(7, struct.pack("<d", 1.25)), 1.25),
        (_primitive(8, bytes([2]) + (-12345).to_bytes(4, "little", signed=True)), Decimal("-123.45")),
        (_primitive(9, bytes([3]) + (123456789012).to_bytes(8, "little", signed=True)), Decimal("123456789.012")),
        (
            _primitive(10, bytes([10]) + (12345678901234567890123456789012345678).to_bytes(16, "little", signed=True)),
            Decimal("1234567890123456789012345678.9012345678"),
        ),
        (_primitive(11, struct.pack("<i", 19723)), date(2024, 1, 1)),
        (_primitive(12, struct.pack("<q", 1704067200123456)), datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)),
        (_primitive(13, struct.pack("<q", 1704067200123456)), datetime(2024, 1, 1, 0, 0, 0, 123456)),
        (_primitive(14, struct.pack("<f", 0.5)), 0.5),
        (_primitive(15, struct.pack("<I", 3) + b"\x00\x01\x02"), b"\x00\x01\x02"),
        (_primitive(16, struct.pack("<I", 5) + "héllo".encode()[:5]), "héll"),
        (_primitive(17, struct.pack("<q", 3_723_000_001)), time(1, 2, 3, 1)),
        (_primitive(18, struct.pack("<q", 1704067200123456789)), datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)),
        (_primitive(19, struct.pack("<q", 1704067200123456789)), datetime(2024, 1, 1, 0, 0, 0, 123456)),
        (
            _primitive(20, uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7").bytes),
            uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        ),
        (_short_string(""), ""),
    ],
)
def test_to_python_primitives(value: bytes, expected: Any) -> None:
    assert to_python(EMPTY_METADATA, value) == expected


def test_to_json_primitives() -> None:
    assert to_json(EMPTY_METADATA, _primitive(1)) == "true"
    assert to_json(EMPTY_METADATA, _primitive(10, bytes([2]) + (10**30).to_bytes(16, "little", signed=True))) == (
        "10000000000000000000000000000.00"
    )
    assert to_json(EMPTY_METADATA, _primitive(11, struct.pack("<i", 19723))) == '"2024-01-01"'
    assert to_json(EMPTY_METADATA, _primitive(15, struct.pack("<I", 3) + b"abc")) == '"YWJj"'
    assert to_json(EMPTY_METADATA, _primitive(7, struct.pack("<d", float("nan")))) == '"nan"'
    # nanosecond timestamps keep all their digits in JSON
    assert to_json(EMPTY_METADATA, _primitive(18, struct.pack("<q", 1704067200123456789))) == (
        '"2024-01-01T00:00:00.123456789+00:00"'
    )
    assert to_json(EMPTY_METADATA, _primitive(19, struct.pack("<q", 1704067200123456789))) == '"2024-01-01T00:00:00.123456789"'
    assert to_json(EMPTY_METADATA, _primitive(16, struct.pack("<I", 7) + b'a"b\\c\nd')) == json.dumps('a"b\\c\nd')


def test_nested_objects_and_arrays() -> None:
    metadata = _metadata("id", "tags", "nested")
    value = _object(
        [
            (0, _primitive(3, b"\x07")),
            (1, _array(_short_string("x"), _primitive(0), _array())),
            (2, _object([(0, _short_string("inner"))])),
        ]
    )

    assert to_python(metadata, value) == {"id": 7, "tags": ["x", None, []], "nested": {"id": "inner"}}
    assert json.loads(to_json(metadata, value)) == {"id": 7, "tags": ["x", None, []], "nested": {"id": "inner"}}


def test_object_with_out_of_order_offsets() -> None:
    # field ids are sorted by key, but values may be laid out in any order
    metadata = _metadata("a", "b")
    values = _short_string("B") + _short_string("A")
    value = bytes([0x02, 2, 0, 1, 2, 0, 4]) + values
    assert to_python(metadata, value) == {"a": "A", "b": "B"}


def test_large_array_and_wide_offsets() -> None:
    elements = [_primitive(3, bytes([index])) for index in range(3)]
    # is_large (4-byte element count) with 2-byte offsets
    header = (0b101 << 2) | 0x03
    value = (
        bytes([header]) + struct.pack("<I", 3) + b"".join(struct.pack("<H", 2 * index) for index in range(4)) + b"".join(elements)
    )
    assert to_python(EMPTY_METADATA, value) == [0, 1, 2]


def test_invalid_variants() -> None:
    with pytest.raises(ValueError, match="Unsupported variant metadata version"):
        to_python(b"\x02\x00\x00", _primitive(0))
    with pytest.raises(ValueError, match="unexpected end of buffer"):
        to_python(EMPTY_METADATA, b"\x35short")
    with pytest.raises(ValueError, match="Unsupported variant primitive type id"):
        to_python(EMPTY_METADATA, _primitive(63))
    with pytest.raises(ValueError, match="not in the metadata dictionary"):
        to_python(EMPTY_METADATA, _object([(0, _primitive(0))]))
