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
import math
import struct

import pytest

from pyiceberg.utils.geo import GeospatialBound, geo_bounds_from_bbox


@pytest.mark.parametrize(
    "bound, expected",
    [
        (GeospatialBound(1.0, 2.0), struct.pack("<2d", 1.0, 2.0)),
        (GeospatialBound(1.0, 2.0, 3.0), struct.pack("<3d", 1.0, 2.0, 3.0)),
        (GeospatialBound(1.0, 2.0, 3.0, 4.0), struct.pack("<4d", 1.0, 2.0, 3.0, 4.0)),
    ],
)
def test_geospatial_bound_round_trip(bound: GeospatialBound, expected: bytes) -> None:
    assert bound.to_bytes() == expected
    assert GeospatialBound.from_bytes(expected) == bound


def test_geospatial_bound_xym_uses_nan_z() -> None:
    serialized = GeospatialBound(1.0, 2.0, m=4.0).to_bytes()
    x, y, z, m = struct.unpack("<4d", serialized)
    assert (x, y, m) == (1.0, 2.0, 4.0)
    assert math.isnan(z)
    assert GeospatialBound.from_bytes(serialized) == GeospatialBound(1.0, 2.0, m=4.0)


def test_geospatial_bound_invalid_length() -> None:
    with pytest.raises(ValueError, match="Invalid geospatial bound of 21 bytes"):
        GeospatialBound.from_bytes(b"\x00" * 21)


def test_geo_bounds_from_bbox() -> None:
    lower, upper = geo_bounds_from_bbox(-3.0, 2.0, 1.0, 5.0)
    assert GeospatialBound.from_bytes(lower) == GeospatialBound(-3.0, 2.0)
    assert GeospatialBound.from_bytes(upper) == GeospatialBound(1.0, 5.0)

    lower, upper = geo_bounds_from_bbox(-3.0, 2.0, 1.0, 5.0, zmin=0.0, zmax=9.0, mmin=None, mmax=7.0)
    # M is only included when both its minimum and maximum are known
    assert GeospatialBound.from_bytes(lower) == GeospatialBound(-3.0, 2.0, 0.0)
    assert GeospatialBound.from_bytes(upper) == GeospatialBound(1.0, 5.0, 9.0)
