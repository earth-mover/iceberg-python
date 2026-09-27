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
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
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
    StringType,
    TimestampNanoType,
    TimestamptzNanoType,
    UnknownType,
    VariantType,
)
from pyiceberg.utils.geo import GeospatialBound
from pyiceberg.variant import to_json as variant_to_json
from pyiceberg.variant import to_python as variant_to_python

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
def test_v3_variant_table_loads(session_catalog: Catalog, spark: "SparkSession", probe_identifier: str) -> None:  # type: ignore[name-defined] # noqa: F821
    spark.sql(f"CREATE TABLE rest.{probe_identifier} (id INT, v VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    spark.sql(f"""INSERT INTO rest.{probe_identifier} SELECT 1, parse_json('{{"a": 1}}')""")
    table = session_catalog.load_table(probe_identifier)
    assert isinstance(table.schema().find_type("v"), VariantType)
    assert table.scan().count() == 1
    (row,) = table.scan().to_arrow().to_pylist()
    assert json.loads(variant_to_json(row["v"]["metadata"], row["v"]["value"])) == {"a": 1}

    spark.sql(
        f"""INSERT INTO rest.{probe_identifier} VALUES
        (2, parse_json('[1, "two", null, 3.5]')),
        (3, parse_json('"just a string"')),
        (4, parse_json('null')),
        (5, NULL),
        (6, parse_json('{{"nested": {{"b": [true, false]}}, "s": "x"}}'))"""
    )
    table = session_catalog.load_table(probe_identifier)
    rows = {row["id"]: row["v"] for row in table.scan().to_arrow().to_pylist()}
    decoded = {id_: None if v is None else variant_to_python(v["metadata"], v["value"]) for id_, v in rows.items()}
    assert decoded == {
        1: {"a": 1},
        2: [1, "two", None, Decimal("3.5")],
        3: "just a string",
        4: None,
        5: None,
        6: {"nested": {"b": [True, False]}, "s": "x"},
    }
    # the JSON matches Spark's rendering of the same values
    expected_json = {r["id"]: r["j"] for r in spark.sql(f"SELECT id, to_json(v) AS j FROM rest.{probe_identifier}").collect()}
    for id_, v in rows.items():
        if v is not None:
            assert json.loads(variant_to_json(v["metadata"], v["value"])) == json.loads(expected_json[id_])

    # variant columns cannot be written until pyarrow can annotate the Parquet VARIANT logical type
    with pytest.raises(NotImplementedError, match="Writing variant is not supported"):
        table.append(table.scan().to_arrow())


# ---------------------------------------------------------------------------- equality deletes


@pytest.mark.integration
def test_v3_equality_deletes_match_spark(session_catalog: Catalog, spark: "SparkSession") -> None:  # type: ignore[name-defined] # noqa: F821
    identifier = "default.test_v3_equality_deletes"
    table = session_catalog.load_table(identifier)
    delete_files = table.inspect.delete_files().to_pylist()
    assert {d["content"] for d in delete_files} == {2}
    assert sorted(d["equality_ids"] for d in delete_files) == [[1], [2]]

    expected = sorted(tuple(r) for r in spark.sql(f"SELECT id, name, value FROM rest.{identifier}").collect())
    # id 2 and 5 are deleted by id, id 4 by its null name; the row added after the deletes is kept
    assert expected == [(1, "a", 1.0), (2, "b2", 20.0), (3, "c", 3.0)]
    actual = sorted((r["id"], r["name"], r["value"]) for r in table.scan().to_arrow().to_pylist())
    assert actual == expected
    assert table.scan().count() == len(expected)

    # the delete columns are applied even when they are not projected
    assert sorted(r["value"] for r in table.scan(selected_fields=("value",)).to_arrow().to_pylist()) == [1.0, 3.0, 20.0]
    assert [r["id"] for r in table.scan(row_filter=EqualTo("id", 2)).to_arrow().to_pylist()] == [2]


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
@pytest.mark.parametrize(
    "identifier", ["default.test_positional_mor_deletes_v3", "default.test_positional_mor_double_deletes_v3"]
)
def test_v3_row_lineage_columns_match_spark(session_catalog: Catalog, spark: "SparkSession", identifier: str) -> None:  # type: ignore[name-defined] # noqa: F821
    table = session_catalog.load_table(identifier)
    spark_rows = spark.sql(f"SELECT number, _row_id, _last_updated_sequence_number FROM rest.{identifier}").collect()
    expected = {r["number"]: (r["_row_id"], r["_last_updated_sequence_number"]) for r in spark_rows}
    scan = table.scan(selected_fields=("number", "_row_id", "_last_updated_sequence_number"))
    got = {r["number"]: (r["_row_id"], r["_last_updated_sequence_number"]) for r in scan.to_arrow().to_pylist()}
    assert got == expected


@pytest.mark.integration
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
def test_v3_inspect_exposes_v3_fields(session_catalog: Catalog) -> None:
    table = session_catalog.load_table("default.test_positional_mor_deletes_v3")
    delete_files = table.inspect.delete_files().to_pylist()
    assert {"referenced_data_file", "content_offset", "content_size_in_bytes"} <= set(delete_files[0])
    assert all(d["referenced_data_file"] and d["content_offset"] is not None for d in delete_files)
    entries = table.inspect.entries().to_pylist()
    assert "first_row_id" in entries[0]["data_file"]


# ---------------------------------------------------------------------------- writes


@pytest.mark.integration
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
    # The pre-upgrade manifest has no first-row-id, so the first v3 commit assigns row ids to its row as well
    assert session_catalog.load_table(probe_identifier).metadata.next_row_id == 2


def _ids(values: list[int]) -> pa.Table:
    return pa.Table.from_arrays([pa.array(values, pa.int32())], schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]))


@pytest.mark.integration
def test_v3_pyiceberg_appends_read_by_spark_with_row_lineage(
    session_catalog: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
    probe_identifier: str,
) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    table.append(_ids([10, 11, 12]))
    table.append(_ids([13, 14]))

    table = session_catalog.load_table(probe_identifier)
    assert table.metadata.next_row_id == 5
    first, second = table.snapshots()
    assert (first.first_row_id, first.added_rows, second.first_row_id, second.added_rows) == (0, 3, 3, 2)

    rows = spark.sql(f"SELECT id, _row_id, _last_updated_sequence_number FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [(r["id"], r["_row_id"]) for r in rows] == [(10, 0), (11, 1), (12, 2), (13, 3), (14, 4)]
    assert [r["_last_updated_sequence_number"] for r in rows] == [first.sequence_number] * 3 + [second.sequence_number] * 2

    # Spark can keep writing to the table PyIceberg wrote
    spark.sql(f"DELETE FROM rest.{probe_identifier} WHERE id = 11")
    rows = spark.sql(f"SELECT id, _row_id FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [(r["id"], r["_row_id"]) for r in rows] == [(10, 0), (12, 2), (13, 3), (14, 4)]
    table = session_catalog.load_table(probe_identifier)
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [10, 12, 13, 14]
    assert table.metadata.next_row_id >= 5


@pytest.mark.integration
def test_v3_pyiceberg_copy_on_write_read_by_spark(
    session_catalog: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
    probe_identifier: str,
) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    table.append(_ids([1, 2, 3]))
    table.append(_ids([4, 5]))
    # Rewrites the first data file: its surviving rows get new row ids, the untouched file keeps its own
    table.delete("id = 2")
    row_ids = {r["id"]: r["_row_id"] for r in spark.sql(f"SELECT id, _row_id FROM rest.{probe_identifier}").collect()}
    assert sorted(row_ids) == [1, 3, 4, 5]
    assert (row_ids[4], row_ids[5]) == (3, 4)
    assert row_ids[1] >= 5 and row_ids[3] >= 5

    table.overwrite(_ids([6]), overwrite_filter="id = 5")
    table = session_catalog.load_table(probe_identifier)
    assert table.metadata.next_row_id == sum(s.added_rows or 0 for s in table.snapshots())
    rows = spark.sql(f"SELECT id, _row_id FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [r["id"] for r in rows] == [1, 3, 4, 6]
    assert len({r["_row_id"] for r in rows}) == 4
    assert all(r["_row_id"] < table.metadata.next_row_id for r in rows)


@pytest.mark.integration
def test_v3_hive_table_written_by_pyiceberg_read_by_spark(
    session_catalog_hive: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
) -> None:
    # Hive stores the metadata PyIceberg serializes, unlike REST where the server builds it
    identifier = f"default.test_v3_hive_{uuid.uuid4().hex[:8]}"
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog_hive.create_table(identifier, schema, properties={"format-version": "3"})
    try:
        assert table.metadata.next_row_id == 0
        table.append(_ids([1, 2]))
        table.append(_ids([3]))
        assert session_catalog_hive.load_table(identifier).metadata.next_row_id == 3

        properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES hive.{identifier}").collect()}
        assert properties["format-version"] == "3"
        rows = spark.sql(f"SELECT id, _row_id FROM hive.{identifier} ORDER BY id").collect()
        assert [(r["id"], r["_row_id"]) for r in rows] == [(1, 0), (2, 1), (3, 2)]
    finally:
        session_catalog_hive.drop_table(identifier)


@pytest.mark.integration
def test_v3_upgrade_then_pyiceberg_append_read_by_spark(
    session_catalog: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
    probe_identifier: str,
) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "2"})
    table.append(_ids([1, 2]))
    with table.transaction() as transaction:
        transaction.upgrade_table_version(3)

    table = session_catalog.load_table(probe_identifier)
    table.append(_ids([3]))
    table = session_catalog.load_table(probe_identifier)
    assert table.metadata.format_version == 3
    snapshot = table.current_snapshot()
    assert snapshot is not None and (snapshot.first_row_id, snapshot.added_rows) == (0, 3)
    assert table.metadata.next_row_id == 3

    rows = spark.sql(f"SELECT id, _row_id FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sorted(r["_row_id"] for r in rows) == [0, 1, 2]


# ---------------------------------------------------------------------------- PyIceberg-written geo, defaults, unknown

WKB_POINT_1_2 = bytes.fromhex("0101000000000000000000f03f0000000000000040")
WKB_POINT_NEG_3_5 = bytes.fromhex("010100000000000000000008c00000000000001440")


@pytest.mark.integration
def test_v3_pyiceberg_geo_write_round_trip(session_catalog: Catalog, probe_identifier: str) -> None:
    ga = pytest.importorskip("geoarrow.pyarrow")
    # Spark 4.0 with Iceberg 1.11 cannot read geometry columns, so the files are verified through PyIceberg and Parquet
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "geom", GeometryType(), required=False),
        NestedField(3, "geom_srid", GeometryType("srid:3857"), required=False),
        NestedField(4, "geog", GeographyType(), required=False),
    )
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    id_field = pa.field("id", pa.int32(), nullable=False)
    plain = pa.array([WKB_POINT_1_2, None], pa.binary())
    plain_schema = pa.schema(
        [id_field, pa.field("geom", pa.binary()), pa.field("geom_srid", pa.binary()), pa.field("geog", pa.binary())]
    )
    table.append(pa.Table.from_arrays([pa.array([1, 2], pa.int32()), plain, plain, plain], schema=plain_schema))

    wkb = pa.array([WKB_POINT_1_2, WKB_POINT_NEG_3_5], pa.binary())
    geo_arrays = [
        pa.ExtensionArray.from_storage(ga.wkb(), wkb),
        pa.ExtensionArray.from_storage(ga.wkb().with_crs("srid:3857"), wkb),
        pa.ExtensionArray.from_storage(ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL), wkb),
    ]
    geo_schema = pa.schema(
        [id_field, *[pa.field(name, array.type) for name, array in zip(["geom", "geom_srid", "geog"], geo_arrays, strict=True)]]
    )
    table.append(pa.Table.from_arrays([pa.array([3, 4], pa.int32()), *geo_arrays], schema=geo_schema))

    table = session_catalog.load_table(probe_identifier)
    result = table.scan().to_arrow().sort_by("id")
    for name in ("geom", "geom_srid", "geog"):
        column = result.column(name)
        assert column.cast(column.type.storage_type).to_pylist() == [WKB_POINT_1_2, None, WKB_POINT_1_2, WKB_POINT_NEG_3_5]

    for task in table.scan().plan_files():
        with table.io.new_input(task.file.file_path).open() as f:
            parquet_schema = str(pq.ParquetFile(f).schema)
        # The Parquet logical types carry the CRS; the default OGC:CRS84 is written as an empty CRS
        assert "field_id=2 geom (Geometry(crs=))" in parquet_schema
        assert "field_id=3 geom_srid (Geometry(crs=srid:3857))" in parquet_schema
        assert "field_id=4 geog (Geography(crs=, algorithm=spherical))" in parquet_schema

    bounds = [
        (
            GeospatialBound.from_bytes(row["readable_metrics"]["geom_srid"]["lower_bound"]),
            GeospatialBound.from_bytes(row["readable_metrics"]["geom_srid"]["upper_bound"]),
        )
        for row in table.inspect.files().to_pylist()
    ]
    bounds.sort(key=lambda bound: bound[0].x)
    assert bounds == [
        (GeospatialBound(-3.0, 2.0), GeospatialBound(1.0, 5.0)),
        (GeospatialBound(1.0, 2.0), GeospatialBound(1.0, 2.0)),
    ]


@pytest.mark.integration
def test_v3_pyiceberg_write_default_read_by_spark(
    session_catalog: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
    probe_identifier: str,
) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    table.append(_ids([1]))
    with table.update_schema() as update:
        update.add_column("color", StringType(), default_value="blue")
    with table.update_schema() as update:
        update.set_default_value("color", "green")
    # The dataframe does not have the column, so the write-default is written
    table.append(_ids([2]))

    table = session_catalog.load_table(probe_identifier)
    assert [row["color"] for row in table.scan().to_arrow().sort_by("id").to_pylist()] == ["blue", "green"]
    rows = spark.sql(f"SELECT id, color FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [(r["id"], r["color"]) for r in rows] == [(1, "blue"), (2, "green")]


@pytest.mark.integration
def test_v3_pyiceberg_unknown_column_append_read_by_spark(
    session_catalog: Catalog,
    spark: "SparkSession",  # type: ignore[name-defined] # noqa: F821
    probe_identifier: str,
) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True), NestedField(2, "unk", UnknownType(), required=False))
    table = session_catalog.create_table(probe_identifier, schema, properties={"format-version": "3"})
    table.append(_ids([1, 2]))

    table = session_catalog.load_table(probe_identifier)
    assert table.scan().to_arrow().sort_by("id").to_pylist() == [{"id": 1, "unk": None}, {"id": 2, "unk": None}]
    (task,) = table.scan().plan_files()
    with table.io.new_input(task.file.file_path).open() as f:
        # Columns of type unknown are not stored in data files
        assert pq.ParquetFile(f).schema_arrow.names == ["id"]
    rows = spark.sql(f"SELECT id, unk FROM rest.{probe_identifier} ORDER BY id").collect()
    assert [(r["id"], r["unk"]) for r in rows] == [(1, None), (2, None)]
