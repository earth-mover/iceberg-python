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

## Support matrix

| Feature | Read | Write | Notes |
|---|---|---|---|
| v3 table metadata (`next-row-id`, `encryption-keys`) | Yes | Yes | v3 tables can be created on every catalog. `next-row-id` starts at 0. |
| Upgrade a table from v1 or v2 to v3 | n/a | Yes | `upgrade_table_version(3)` sets `next-row-id` to 0 and leaves existing snapshots untouched. |
| Manifests and manifest lists | Yes | Yes | Data and delete manifests; manifest lists assign `first-row-id` to data manifests. |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | No | Yes | Commits set the snapshot `first-row-id` and `added-rows` and advance `next-row-id`, so engines such as Spark read spec-correct row ids. The `_row_id` and `_last_updated_sequence_number` columns cannot be read with PyIceberg yet. |
| Deletion vectors (Puffin) | Yes | No | Read support decodes every blob in the file. Deletes on v3 tables are copy-on-write. |
| Position delete files (v2 style) | Yes | No | |
| Equality deletes | No | No | |
| `timestamp_ns` / `timestamptz_ns` | Yes | No | Values are read with nanosecond precision. Row filters and partition pruning work on nanosecond columns. |
| `unknown` | Yes | No | Reads as null. |
| `variant` | No | No | Tables with a variant column cannot be loaded yet. |
| `geometry` / `geography` | Yes | No | Type strings are parsed and serialized in the Java/spec form, e.g. `geometry(srid:3857)`. Only equality and null predicates are supported, and bounds are not used to skip files. See [Geospatial types](geospatial.md). |
| Default values (`initial-default`, `write-default`) | Yes | Partial | `initial-default` is applied on read. Adding a column with a default value requires a v3 table. |
| Multi-argument transforms | No | No | Loading a table whose spec uses `source-ids` with more than one id raises. |
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

Schemas that use v3-only types (`timestamp_ns`, `timestamptz_ns`, `unknown`, `geometry`, `geography`) are rejected on
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
