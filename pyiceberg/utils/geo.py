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
"""Helpers for geometry and geography column bounds.

Iceberg stores the lower and upper bounds of a geometry or geography column as the corner points of the
bounding box. Each point is serialized as the concatenation of little-endian IEEE 754 doubles
``x:y`` (16 bytes), ``x:y:z`` (24 bytes) or ``x:y:z:m`` (32 bytes); when only ``m`` is present, ``z`` is NaN.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass


@dataclass(frozen=True)
class GeospatialBound:
    """A corner point of a geospatial bounding box."""

    x: float
    y: float
    z: float | None = None
    m: float | None = None

    def to_bytes(self) -> bytes:
        """Serialize the point to the Iceberg geospatial bound encoding."""
        if self.m is not None:
            z = self.z if self.z is not None else math.nan
            return struct.pack("<4d", self.x, self.y, z, self.m)
        if self.z is not None:
            return struct.pack("<3d", self.x, self.y, self.z)
        return struct.pack("<2d", self.x, self.y)

    @staticmethod
    def from_bytes(buf: bytes) -> GeospatialBound:
        """Deserialize a point from the Iceberg geospatial bound encoding."""
        if len(buf) == 16:
            x, y = struct.unpack("<2d", buf)
            return GeospatialBound(x, y)
        if len(buf) == 24:
            x, y, z = struct.unpack("<3d", buf)
            return GeospatialBound(x, y, z)
        if len(buf) == 32:
            x, y, z, m = struct.unpack("<4d", buf)
            return GeospatialBound(x, y, None if math.isnan(z) else z, m)
        raise ValueError(f"Invalid geospatial bound of {len(buf)} bytes, expected 16, 24 or 32")


def geo_bounds_from_bbox(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    zmin: float | None = None,
    zmax: float | None = None,
    mmin: float | None = None,
    mmax: float | None = None,
) -> tuple[bytes, bytes]:
    """Encode a bounding box as the serialized (lower, upper) bound points.

    Z and M are only included when both their minimum and maximum are known.
    """
    has_z = zmin is not None and zmax is not None
    has_m = mmin is not None and mmax is not None
    lower = GeospatialBound(xmin, ymin, zmin if has_z else None, mmin if has_m else None)
    upper = GeospatialBound(xmax, ymax, zmax if has_z else None, mmax if has_m else None)
    return lower.to_bytes(), upper.to_bytes()
