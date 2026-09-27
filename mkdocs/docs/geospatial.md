# Geospatial Types

PyIceberg models the Iceberg v3 geospatial primitive types `geometry` and `geography`. Support is a work in progress; the [format version 3](format-version-3.md) page lists what works today.

## Overview

Iceberg v3 introduces native support for spatial data types:

- **`geometry(C)`**: Represents geometric shapes in a coordinate reference system (CRS)
- **`geography(C, A)`**: Represents geographic shapes with CRS and calculation algorithm

Both types store values as WKB (Well-Known Binary) bytes.

## Requirements

- Iceberg format version 3 or higher
- Optional: `geoarrow-pyarrow` for GeoArrow extension type metadata and interoperability. Without it, geometry and geography are written as binary in Parquet while the Iceberg schema still preserves the spatial type. Install with `pip install pyiceberg[geoarrow]`.

## Usage

### Declaring Columns

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, GeometryType, GeographyType

# Schema with geometry and geography columns
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "location", GeometryType(), required=True),
    NestedField(3, "boundary", GeographyType(), required=False),
)
```

### Type Parameters

#### GeometryType

```python
# Default CRS (OGC:CRS84)
GeometryType()

# Custom CRS
GeometryType("EPSG:4326")
```

#### GeographyType

```python
# Default CRS (OGC:CRS84) and algorithm (spherical)
GeographyType()

# Custom CRS
GeographyType("EPSG:4326")

# Custom CRS and edge-interpolation algorithm: spherical (default), vincenty, thomas, andoyer or karney
GeographyType("EPSG:4326", "vincenty")
```

### String Type Syntax

Types can also be specified as strings in schema definitions, using the same syntax as the Iceberg spec and the
Java implementation:

```python
# Using string type names
NestedField(1, "point", "geometry", required=True)
NestedField(2, "region", "geography", required=True)

# With parameters
NestedField(3, "location", "geometry(srid:3857)", required=True)
NestedField(4, "boundary", "geography(srid:4326, vincenty)", required=True)
```

Type strings are case-insensitive. Types are written to table metadata in the same unquoted form, for example
`geometry(srid:3857)` and `geography(srid:4326, vincenty)`; a default CRS (`OGC:CRS84`) and algorithm (`spherical`)
are omitted. Metadata written by older PyIceberg versions, which quoted the parameters (`geometry('srid:3857')`),
is still parsed.

## Data Representation

Values are represented as WKB (Well-Known Binary) bytes at runtime:

```python
# Example: Point(0, 0) in WKB format
point_wkb = bytes.fromhex("0101000000000000000000000000000000000000")
```

## Reading Tables Written by Other Engines

Geometry and geography tables written by the Java implementation (for example through Spark or Sedona) can be
scanned. Parquet `GEOMETRY`/`GEOGRAPHY` columns, and plain binary columns holding WKB, are read according to the
Iceberg schema. With `geoarrow-pyarrow` installed the resulting Arrow columns use the GeoArrow `geoarrow.wkb`
extension type with the column's CRS (and edge type for geography); without it they are `large_binary`.

```python
from pyiceberg.expressions import EqualTo

table = catalog.load_table("db.spatial_table")
arrow_table = table.scan(row_filter=EqualTo("location", point_wkb)).to_arrow()
```

Row filters on geometry and geography columns support `EqualTo`, `NotEqualTo`, `In`, `NotIn`, `IsNull` and
`NotNull`, which compare WKB bytes. Ordering predicates such as `LessThan` raise a `ValueError`. In the
`readable_metrics` of `table.inspect.files()` and `table.inspect.entries()`, geometry and geography bounds are shown
as raw bytes.

## Current Limitations

1. **WKB/WKT Conversion**: Converting between WKB bytes and WKT strings requires external libraries (like Shapely). PyIceberg does not include this conversion to avoid heavy dependencies.

2. **Spatial Predicates**: Spatial filtering (e.g., ST_Contains, ST_Intersects) is not yet supported for query pushdown.

3. **Bounds Metrics**: Geometry/geography bounds (bounding boxes) are not used to skip data files, and PyIceberg does not
   write them.

4. **Without geoarrow-pyarrow**: When the `geoarrow-pyarrow` package is not installed, geometry and geography columns are stored as binary without GeoArrow extension type metadata. The Iceberg schema preserves type information, but other tools reading the Parquet files directly may not recognize them as spatial types. Install with `pip install pyiceberg[geoarrow]` for full GeoArrow support.

5. **Writing**: Writing to format version 3 tables, and therefore writing geometry or geography data, is not yet supported (see [format version 3](format-version-3.md)).

## Format Version

Geometry and geography types require Iceberg format version 3:

```python
from pyiceberg.table import TableProperties

# Creating a v3 table
table = catalog.create_table(
    identifier="db.spatial_table",
    schema=schema,
    properties={
        TableProperties.FORMAT_VERSION: "3"
    }
)
```

Attempting to use these types with format version 1 or 2 will raise a validation error.
