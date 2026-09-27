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
| v3 table metadata (`next-row-id`, `encryption-keys`) | Yes | No | Writing v3 metadata is tracked in [#1551](https://github.com/apache/iceberg-python/issues/1551). Only REST catalogs, where the server writes metadata, can create v3 tables. |
| Upgrade a table from v2 to v3 | n/a | No | `upgrade_table_version(3)` is rejected. |
| Manifests and manifest lists | Yes | No | |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | No | No | Snapshot `first-row-id` / `added-rows` are parsed. |
| Deletion vectors (Puffin) | Yes | No | Read support decodes every blob in the file. |
| Position delete files (v2 style) | Yes | No | |
| Equality deletes | No | No | |
| `timestamp_ns` / `timestamptz_ns` | Yes | No | Values are read with nanosecond precision. Predicates on nanosecond columns are not yet supported. |
| `unknown` | Yes | No | Reads as null. |
| `variant` | No | No | Tables with a variant column cannot be loaded yet. |
| `geometry` / `geography` | Partial | No | Types with the default CRS parse; the spec form `geometry(srid:3857)` does not yet. See [Geospatial types](geospatial.md). |
| Default values (`initial-default`, `write-default`) | Yes | Partial | `initial-default` is applied on read. |
| Multi-argument transforms | No | No | Loading a table whose spec uses `source-ids` with more than one id raises. |
| Table encryption | Metadata only | No | `encryption-keys` and `key-id` round-trip through metadata. |

## Creating a v3 table

Pass `format-version` as a table property. This currently only succeeds with a REST catalog:

```python
from pyiceberg.table import TableProperties

table = catalog.create_table(
    identifier="default.events",
    schema=schema,
    properties={TableProperties.FORMAT_VERSION: "3"},
)
```

Schemas that use v3-only types (`timestamp_ns`, `timestamptz_ns`, `unknown`, `geometry`, `geography`) are rejected on
v1 and v2 tables.

## Nanosecond timestamps

On v3 tables, PyArrow `timestamp('ns')` columns map to `timestamp_ns` / `timestamptz_ns` and are read back at full
precision. On v1 and v2 tables nanosecond input must be downcast with the `downcast-ns-timestamp-to-us-on-write`
property (see [Configuration](configuration.md#nanoseconds-support)).
