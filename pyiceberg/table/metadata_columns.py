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
"""Reserved metadata columns that can be requested in a scan projection.

See https://iceberg.apache.org/spec/#reserved-field-ids and https://iceberg.apache.org/spec/#row-lineage.
"""

from __future__ import annotations

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

ROW_ID = NestedField(
    field_id=2147483540,
    name="_row_id",
    field_type=LongType(),
    required=False,
    doc="Implicit row ID that is automatically assigned",
)
LAST_UPDATED_SEQUENCE_NUMBER = NestedField(
    field_id=2147483539,
    name="_last_updated_sequence_number",
    field_type=LongType(),
    required=False,
    doc="Sequence number when the row was last updated",
)

ROW_LINEAGE_COLUMNS: tuple[NestedField, ...] = (ROW_ID, LAST_UPDATED_SEQUENCE_NUMBER)
_METADATA_COLUMNS_BY_NAME: dict[str, NestedField] = {field.name: field for field in ROW_LINEAGE_COLUMNS}
_METADATA_COLUMNS_BY_ID: dict[int, NestedField] = {field.field_id: field for field in ROW_LINEAGE_COLUMNS}


def _lookup(name: str, case_sensitive: bool = True) -> NestedField | None:
    if case_sensitive:
        return _METADATA_COLUMNS_BY_NAME.get(name)
    return next((field for field in ROW_LINEAGE_COLUMNS if field.name.lower() == name.lower()), None)


def is_metadata_column(name: str, case_sensitive: bool = True) -> bool:
    """Return whether the name refers to a supported reserved metadata column."""
    return _lookup(name, case_sensitive) is not None


def is_metadata_field_id(field_id: int) -> bool:
    """Return whether the field ID belongs to a supported reserved metadata column."""
    return field_id in _METADATA_COLUMNS_BY_ID


def project_schema(schema: Schema, selected_fields: tuple[str, ...], case_sensitive: bool = True) -> Schema:
    """Select columns from a table schema, appending any requested metadata columns in the requested order.

    Args:
        schema: The table schema to select from.
        selected_fields: Column names to select; ``*`` selects all table columns.
        case_sensitive: Whether name lookups are case-sensitive.

    Returns:
        The projected schema, with metadata columns after the table columns.
    """
    table_fields: list[str] = []
    metadata_fields: list[NestedField] = []
    for name in selected_fields:
        metadata_field = _lookup(name, case_sensitive)
        # A table column with the same name takes precedence over the metadata column
        if metadata_field is not None and not _has_field(schema, name, case_sensitive):
            if metadata_field not in metadata_fields:
                metadata_fields.append(metadata_field)
        else:
            table_fields.append(name)

    if "*" in table_fields:
        projected = schema
    elif table_fields:
        projected = schema.select(*table_fields, case_sensitive=case_sensitive)
    else:
        projected = Schema(schema_id=schema.schema_id)

    if not metadata_fields:
        return projected

    return Schema(
        *projected.fields,
        *metadata_fields,
        schema_id=projected.schema_id,
        identifier_field_ids=projected.identifier_field_ids,
    )


def _has_field(schema: Schema, name: str, case_sensitive: bool) -> bool:
    try:
        schema.find_field(name, case_sensitive)
        return True
    except ValueError:
        return False
