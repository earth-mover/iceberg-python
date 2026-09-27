<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Format version 3

<!-- markdown-link-check-disable-next-line -->
The [Iceberg table spec](https://iceberg.apache.org/spec/) version 3 adds new column types, default values,
row lineage, deletion vectors, multi-argument transforms, and table encryption. This page tracks what PyIceberg
supports today. The table is kept in sync with the code; a feature listed as unsupported raises a clear error rather
than silently producing incompatible files.

The `files`, `data_files`, `delete_files`, `entries` and `all_*` inspect tables include the v3 content file fields
`first_row_id`, `referenced_data_file`, `content_offset` and `content_size_in_bytes`.

## Support matrix

| Feature | Read | Write | Notes |
|---|---|---|---|
| v3 table metadata (`next-row-id`, `encryption-keys`) | Yes | Yes | v3 tables can be created on every catalog. `next-row-id` starts at 0. |
| Upgrade a table from v1 or v2 to v3 | n/a | Yes | `upgrade_table_version(3)` sets `next-row-id` to 0 and leaves existing snapshots untouched. |
| Manifests and manifest lists | Yes | Yes | Data and delete manifests; manifest lists assign `first-row-id` to data manifests. |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | Yes | Yes | Data files inherit `first_row_id` from their manifest, and both columns can be requested in `selected_fields` (see [Row lineage metadata columns](api.md#row-lineage-metadata-columns)). Commits set the snapshot `first-row-id` and `added-rows` and advance `next-row-id`. Row ids and last updated sequence numbers are preserved across copy-on-write rewrites (`delete`, `overwrite`, `upsert`), which store them in the rewritten files, and across merge-on-read deletes; rows changed by `upsert` get new row ids. |
| Deletion vectors (Puffin) | Yes | Yes | Only the blob range named by `content_offset` / `content_size_in_bytes` is read, and the blob length, magic, CRC-32 and cardinality are validated. A DV applies only to its `referenced_data_file` and replaces older position deletes for that file. `delete()` writes DVs when `write.delete.mode` is `merge-on-read` (see [Merge-on-read deletes](#merge-on-read-deletes)); `overwrite()` and `upsert()` are copy-on-write. |
| Position delete files (v2 style) | Yes | No | v3 does not allow new position delete files; deletion vectors replace them. |
| Equality deletes | Yes | No | Equality delete files apply to data files with a lower data sequence number in the same partition, or in every partition when written with an unpartitioned spec. A null delete value matches a null data value. `count()` honours them. |
| `timestamp_ns` / `timestamptz_ns` | Yes | Yes | Values are read and written with nanosecond precision (Parquet INT64 nanoseconds, nanosecond-exact bounds). Row filters and partition pruning work on nanosecond columns. |
| `unknown` | Yes | Yes | Reads as null. Unknown columns must be optional with a null default and are not stored in data files. |
| `variant` | Yes (unshredded) | No | Read as a struct of `metadata` and `value` binaries; decode with `pyiceberg.variant` (see [Variant columns](api.md#variant-columns)). Shredded files raise `NotImplementedError` when the column is read. Writing waits on PyArrow support for the Parquet `VARIANT` logical type. Variant cannot be a partition or sort source. |
| `geometry` / `geography` | Yes | Yes | Type strings are parsed and serialized in the Java/spec form, e.g. `geometry(srid:3857)`. WKB can be appended as binary or GeoArrow columns; with `geoarrow-pyarrow` installed the Parquet `GEOMETRY`/`GEOGRAPHY` logical types are written. Geometry bounds are written as bounding-box points. Only equality and null predicates are supported, and bounds are not used to skip files. See [Geospatial types](geospatial.md). |
| Default values (`initial-default`, `write-default`) | Yes | Yes | `initial-default` is applied on read and `write-default` fills columns missing from appended data, so a required column with a `write-default` may be left out. Adding a column with a default value requires a v3 table. `unknown`, `geometry` and `geography` columns must default to null. |
| Type promotions | Yes | Yes | `unknown` to any primitive type and `date` to `timestamp` / `timestamp_ns` are only allowed on v3 tables; `date` promotion is rejected when the column is partitioned by a transform other than `year`, `month` or `day`. |
| Multi-argument transforms | Yes (as unknown transforms) | No | Fields with several `source-ids` load as unknown transforms and round-trip `source-ids`; their partition values are not used to prune files. |
| Table encryption | Metadata only | No | `encryption-keys` and `key-id` round-trip through metadata. |

## Creating a v3 table

Pass `format-version` as a table property. This works on every catalog:

```python
from pyiceberg.table import TableProperties

table = catalog.create_table(
    identifier="default.events",
    schema=schema,
    properties={TableProperties.FORMAT_VERSION: "3"},
)
```

An existing v1 or v2 table can be upgraded in place. Rows written before the upgrade have no row ids until the next
commit assigns them:

```python
with table.transaction() as transaction:
    transaction.upgrade_table_version(3)
```

Schemas that use v3-only types (`timestamp_ns`, `timestamptz_ns`, `unknown`, `variant`, `geometry`, `geography`) are rejected on
v1 and v2 tables, both on create and when a column is added.

## Nanosecond timestamps

On v3 tables, PyArrow `timestamp('ns')` columns map to `timestamp_ns` / `timestamptz_ns` and are read back at full
precision. On v1 and v2 tables nanosecond input must be downcast with the `downcast-ns-timestamp-to-us-on-write`
property (see [Configuration](configuration.md#nanoseconds-support)).

Row filters on nanosecond columns accept ISO-8601 strings with up to nine fractional digits, integers (nanoseconds
since the epoch), `date` and `datetime` values. Python `datetime` objects only hold microseconds, so use a string or an
integer to filter on sub-microsecond values:

```python
from pyiceberg.expressions import GreaterThanOrEqual

table.scan(row_filter=GreaterThanOrEqual("ts_ns", "2024-01-01T00:00:00.123456789"))
table.scan(row_filter=GreaterThanOrEqual("tstz_ns", "2024-01-01T00:00:00.123456789+00:00"))
```

As with microsecond timestamps, `timestamptz_ns` filters require a zone offset and `timestamp_ns` filters reject one.

## Merge-on-read deletes

With the `write.delete.mode` table property set to `merge-on-read`, `delete()` on a v3 table marks the matching rows
of a data file as deleted in a deletion vector instead of rewriting the file:

```python
table = catalog.create_table(
    identifier="default.events",
    schema=schema,
    properties={"format-version": "3", "write.delete.mode": "merge-on-read"},
)
table.delete("id == 2")
```

- Data files whose rows all match the filter are still dropped as a whole.
- All deletion vectors of a commit are written to one Puffin file, one `deletion-vector-v1` blob per data file.
- A data file has at most one deletion vector. Deleting more rows from a file that already has one writes a new vector
  with the union of the deleted positions and removes the old one in the same commit (`removed-dvs` in the snapshot
  summary). Older file-scoped position delete files are merged in and removed as well.
- The snapshot operation is `delete` and no rows are added, so row ids of the remaining rows are unchanged.
- The commit is validated against concurrent commits: it fails with a `ValidationException` if another writer
  removed one of the data files or added deletes for it.
