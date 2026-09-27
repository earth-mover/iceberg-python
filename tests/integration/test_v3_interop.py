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
"""Format-version 3 interoperability tests against tables written by the Java reference implementation.

Fixtures come from ``dev/provision.py`` (Spark SQL) and ``dev/provision_v3.scala`` (Java Table API, for the
features Spark SQL cannot produce). Each test asserts spec-correct behaviour; the ones marked ``xfail(strict=True)``
document known gaps and must be un-marked when the gap is closed.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    GeographyType,
    GeometryType,
    IntegerType,
    NestedField,
    TimestampNanoType,
    TimestamptzNanoType,
    UnknownType,
)

NS_VALUES = [1704067200123456789, 1706832000000000001, None]
NS_PARTITIONS = [
    # (id, year(ts), month(tstz), bucket(tstz, 4)) as computed by the Java transforms
    (1, 53, 647, 0),
    (2, 54, 648, 0),
    (3, 54, 652, 1),
]


@pytest.fixture
def probe_identifier(session_catalog: Catalog) -> Iterator[str]:
    name = f"default.test_v3_probe_{uuid.uuid4().hex[:8]}"
    yield name
    try:
        session_catalog.drop_table(name)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------- nanosecond timestamps


@pytest.mark.integration
def test_v3_ns_timestamps_read(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_ns_timestamps")
    assert table.metadata.format_version == 3
    assert isinstance(table.schema().find_type("ts_ns"), TimestampNanoType)
    assert isinstance(table.schema().find_type("tstz_ns"), TimestamptzNanoType)
    arrow = table.scan().to_arrow().sort_by("id")
    assert arrow.schema.field("ts_ns").type == pa.timestamp("ns")
    assert arrow.schema.field("tstz_ns").type == pa.timestamp("ns", tz="UTC")
    assert arrow.column("ts_ns").cast(pa.int64()).to_pylist() == NS_VALUES
    assert arrow.column("tstz_ns").cast(pa.int64()).to_pylist() == NS_VALUES
    assert arrow.column("ts_us").to_pylist() == [datetime(2024, 1, 1, 0, 0, 0, 123456), datetime(2024, 2, 2), None]


@pytest.mark.integration
@pytest.mark.parametrize(
    "row_filter, expected",
    [
        (GreaterThanOrEqual("ts_ns", "2024-02-01T00:00:00"), [2]),
        (LessThan("ts_ns", "2024-01-01T00:00:00.500000"), [1]),
        (GreaterThanOrEqual("ts_ns", datetime(2024, 2, 1)), [2]),
        (GreaterThanOrEqual("tstz_ns", datetime(2024, 2, 1, tzinfo=timezone.utc)), [2]),
        (GreaterThanOrEqual("tstz_ns", "2024-02-01T00:00:00+00:00"), [2]),
    ],
)
def test_v3_ns_timestamp_filters(session_catalog: Catalog, row_filter: object, expected: list[int]) -> None:
    table = session_catalog.load_table("default.test_v3_ns_timestamps")
    assert sorted(r["id"] for r in table.scan(row_filter=row_filter).to_arrow().to_pylist()) == expected  # type: ignore[arg-type]


@pytest.mark.integration
def test_v3_ns_partition_transforms_match_java(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_ns_partitions")
    schema = table.schema()
    arrow = table.scan().to_arrow().sort_by("id")
    ts = arrow.column("ts").cast(pa.int64()).to_pylist()
    tstz = arrow.column("tstz").cast(pa.int64()).to_pylist()
    for (row_id, year, month, bucket), ts_value, tstz_value in zip(NS_PARTITIONS, ts, tstz, strict=True):
        assert YearTransform().transform(schema.find_type("ts"))(ts_value) == year, row_id
        assert MonthTransform().transform(schema.find_type("tstz"))(tstz_value) == month, row_id
        assert BucketTransform(4).transform(schema.find_type("tstz"))(tstz_value) == bucket, row_id
    files = table.inspect.files().to_pylist()
    partitions = sorted((f["partition"]["ts_year"], f["partition"]["tstz_month"], f["partition"]["tstz_bucket"]) for f in files)
    assert partitions == sorted((y, m, b) for _, y, m, b in NS_PARTITIONS)


@pytest.mark.integration
def test_v3_ns_partition_pruning(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_ns_partitions")
    scan = table.scan(row_filter=GreaterThanOrEqual("ts", "2024-01-01T00:00:00"))
    assert len(list(scan.plan_files())) == 2
    assert sorted(r["id"] for r in scan.to_arrow().to_pylist()) == [2, 3]


# ---------------------------------------------------------------------------- geometry / geography


@pytest.mark.integration
def test_v3_geo_schema_from_java(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_geo")
    schema = table.schema()
    assert isinstance(schema.find_type("geom"), GeometryType)
    assert schema.find_type("geom_srid") == GeometryType("srid:3857")
    assert schema.find_type("geog") == GeographyType()
    assert schema.find_type("geog_v") == GeographyType("srid:4326", "vincenty")


@pytest.mark.integration
def test_v3_geo_types_created_through_rest_match_java_serialization(session_catalog: Catalog, probe_identifier: str) -> None:
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "geom_srid", GeometryType("srid:3857"), required=False),
        NestedField(3, "geog_v", GeographyType("srid:4326", "vincenty"), required=False),
    )
    session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    loaded = session_catalog.load_table(probe_identifier)
    assert loaded.schema().find_type("geom_srid") == GeometryType("srid:3857")
    assert loaded.schema().find_type("geog_v") == GeographyType("srid:4326", "vincenty")


@pytest.mark.integration
def test_v3_geo_metadata_written_by_java_server_uses_spec_type_strings(session_catalog: Catalog, probe_identifier: str) -> None:
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "geom", GeometryType(), required=False),
        NestedField(3, "geom_srid", GeometryType("srid:3857"), required=False),
        NestedField(4, "geog_v", GeographyType("srid:4326", "vincenty"), required=False),
    )
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    io = PyArrowFileIO(
        properties={"s3.endpoint": "http://localhost:9000", "s3.access-key-id": "admin", "s3.secret-access-key": "password"}
    )
    with io.new_input(table.metadata_location).open() as f:
        metadata = json.loads(f.read())
    # The metadata file is written by the Java REST server, so it shows how Java understood the type strings
    types = {field["name"]: field["type"] for field in metadata["schemas"][-1]["fields"]}
    assert types == {
        "id": "int",
        "geom": "geometry",
        "geom_srid": "geometry(srid:3857)",
        "geog_v": "geography(srid:4326, vincenty)",
    }


# ---------------------------------------------------------------------------- default values


@pytest.mark.integration
def test_v3_initial_defaults_applied_on_read(session_catalog: Catalog, spark: "SparkSession") -> None:  # type: ignore[name-defined] # noqa: F821
    table = session_catalog.load_table("default.test_v3_defaults")
    assert table.schema().find_field("color").initial_default == "blue"
    assert table.schema().find_field("color").write_default == "green"
    rows = table.scan().to_arrow().sort_by("id").to_pylist()
    spark_rows = spark.sql("SELECT * FROM rest.default.test_v3_defaults ORDER BY id").collect()
    assert len(rows) == len(spark_rows) == 2
    for row, spark_row in zip(rows, spark_rows, strict=True):
        assert row["color"] == spark_row["color"] == "blue"
        assert row["qty"] == spark_row["qty"] == 42
        assert row["ratio"] == spark_row["ratio"] == 1.5
        assert str(row["d"]) == str(spark_row["d"]) == "2024-03-04"
        assert row["ts"] == datetime(2024, 3, 4, 5, 6, 7, tzinfo=timezone.utc)
        assert str(row["dec"]) == "12.34"
        assert row["b"] is True
        assert row["bin"] == b"\x01\x02"
        assert str(row["u"]) == "f79c3e09-677c-4bbd-a479-3f349cb785e7"
        assert row["req"] == 7
    assert table.scan(row_filter=EqualTo("color", "blue")).count() == 2


@pytest.mark.integration
def test_v3_nested_struct_default_applied_on_read(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_nested_defaults")
    assert table.scan().to_arrow().to_pylist() == [{"id": 1, "s": {"a": 1, "b": "x", "c": 99}}]


# ---------------------------------------------------------------------------- unknown


@pytest.mark.integration
def test_v3_unknown_column_reads_as_null(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_v3_unknown")
    assert isinstance(table.schema().find_type("unk"), UnknownType)
    assert table.scan().to_arrow().sort_by("id").to_pylist() == [{"id": 1, "unk": None}, {"id": 2, "unk": None}]


# ---------------------------------------------------------------------------- variant


@pytest.mark.integration
@pytest.mark.xfail(strict=True, reason="VariantType is not implemented")
def test_v3_variant_table_loads(session_catalog: Catalog, spark: "SparkSession", probe_identifier: str) -> None:  # type: ignore[name-defined] # noqa: F821
    spark.sql(f"CREATE TABLE rest.{probe_identifier} (id INT, v VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    spark.sql(f"""INSERT INTO rest.{probe_identifier} SELECT 1, parse_json('{{"a": 1}}')""")
    table = session_catalog.load_table(probe_identifier)
    assert table.scan().count() == 1


# ---------------------------------------------------------------------------- row lineage and deletion vectors


@pytest.mark.integration
def test_v3_deletion_vectors_match_spark(session_catalog: Catalog, spark: "SparkSession") -> None:  # type: ignore[name-defined] # noqa: F821
    table = session_catalog.load_table("default.test_positional_mor_deletes_v3")
    assert table.metadata.format_version == 3
    delete_files = table.inspect.delete_files().to_pylist()
    assert delete_files and {d["file_format"] for d in delete_files} == {"PUFFIN"}
    expected = sorted(r["number"] for r in spark.sql("SELECT number FROM rest.default.test_positional_mor_deletes_v3").collect())
    assert sorted(r["number"] for r in table.scan().to_arrow().to_pylist()) == expected
    assert table.metadata.next_row_id == sum(s.added_rows or 0 for s in table.snapshots())


@pytest.mark.integration
@pytest.mark.xfail(strict=True, reason="_row_id / _last_updated_sequence_number metadata columns are not implemented")
def test_v3_row_lineage_columns_match_spark(session_catalog: Catalog, spark: "SparkSession") -> None:  # type: ignore[name-defined] # noqa: F821
    table = session_catalog.load_table("default.test_positional_mor_deletes_v3")
    spark_rows = spark.sql(
        "SELECT number, _row_id, _last_updated_sequence_number FROM rest.default.test_positional_mor_deletes_v3"
    ).collect()
    expected = {r["number"]: (r["_row_id"], r["_last_updated_sequence_number"]) for r in spark_rows}
    scan = table.scan(selected_fields=("number", "_row_id", "_last_updated_sequence_number"))
    got = {r["number"]: (r["_row_id"], r["_last_updated_sequence_number"]) for r in scan.to_arrow().to_pylist()}
    assert got == expected


@pytest.mark.integration
@pytest.mark.xfail(strict=True, reason="first_row_id is not inherited from the manifest on read")
def test_v3_first_row_id_inherited(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_positional_mor_deletes_v3")
    snapshot = table.current_snapshot()
    assert snapshot is not None
    for manifest in snapshot.manifests(table.io):
        if manifest.content.name == "DATA":
            assert manifest.first_row_id is not None
            for entry in manifest.fetch_manifest_entry(table.io):
                assert entry.data_file.first_row_id is not None


@pytest.mark.integration
@pytest.mark.xfail(strict=True, reason="inspect tables do not expose the v3 data file fields")
def test_v3_inspect_exposes_v3_fields(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_positional_mor_deletes_v3")
    delete_files = table.inspect.delete_files().to_pylist()
    assert {"referenced_data_file", "content_offset", "content_size_in_bytes"} <= set(delete_files[0])
    assert all(d["referenced_data_file"] and d["content_offset"] is not None for d in delete_files)
    entries = table.inspect.entries().to_pylist()
    assert "first_row_id" in entries[0]["data_file"]


# ---------------------------------------------------------------------------- writes


@pytest.mark.integration
@pytest.mark.xfail(strict=True, raises=ValueError, reason="Writing v3 manifests is not supported")
def test_v3_append_to_rest_table(session_catalog: Catalog, probe_identifier: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    assert table.metadata.next_row_id == 0
    df = pa.Table.from_arrays([pa.array([1, 2], pa.int32())], schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]))
    table.append(df)
    table = session_catalog.load_table(probe_identifier)
    assert table.scan().count() == 2
    snapshot = table.current_snapshot()
    assert snapshot is not None and snapshot.first_row_id == 0 and snapshot.added_rows == 2
    assert table.metadata.next_row_id == 2


@pytest.mark.integration
@pytest.mark.xfail(strict=True, raises=ValueError, reason="Upgrading to format version 3 is not supported")
def test_v3_upgrade_from_v2(session_catalog: Catalog, spark: "SparkSession", probe_identifier: str) -> None:  # type: ignore[name-defined] # noqa: F821
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    session_catalog.create_table(probe_identifier, schema, properties={"format-version": "2"})
    spark.sql(f"INSERT INTO rest.{probe_identifier} VALUES (1)")
    table = session_catalog.load_table(probe_identifier)
    with table.transaction() as transaction:
        transaction.upgrade_table_version(3)
    table = session_catalog.load_table(probe_identifier)
    assert table.metadata.format_version == 3
    assert table.metadata.next_row_id == 0
    spark.sql(f"INSERT INTO rest.{probe_identifier} VALUES (2)")
    assert session_catalog.load_table(probe_identifier).metadata.next_row_id == 1
