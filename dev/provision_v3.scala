// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Provisions format-version 3 fixtures that Spark SQL cannot create (nanosecond timestamps,
// geometry/geography, column defaults, unknown) through the Iceberg Java Table API.
// Executed by dev/provision.py inside the Spark container:
//   spark-shell --master local[1] < /tmp/provision_v3.scala

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}

import org.apache.iceberg.{DataFiles, FileFormat, PartitionKey, PartitionSpec, Schema, Table}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord, InternalRecordWrapper, Record}
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.types.Types

import scala.collection.JavaConverters._

val catalog = new RESTCatalog()
catalog.initialize(
  "rest",
  Map(
    "uri" -> "http://rest:8181",
    "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
    "s3.endpoint" -> "http://object-store:9000",
    "warehouse" -> "s3://warehouse/rest/"
  ).asJava
)

val v3 = Map("format-version" -> "3").asJava

def recreate(name: String, schema: Schema, spec: PartitionSpec = PartitionSpec.unpartitioned()): Table = {
  val ident = TableIdentifier.of("default", name)
  if (catalog.tableExists(ident)) catalog.dropTable(ident, true)
  catalog.createTable(ident, schema, spec, v3)
}

def appendRecords(table: Table, records: Seq[Record], fileName: String): Unit = {
  val spec = table.spec()
  val grouped = records.groupBy { r =>
    val key = new PartitionKey(spec, table.schema())
    key.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(r))
    key
  }
  val append = table.newAppend()
  grouped.zipWithIndex.foreach { case ((key, recs), i) =>
    val path =
      if (spec.isPartitioned) table.locationProvider().newDataLocation(spec, key, s"$fileName-$i.parquet")
      else table.locationProvider().newDataLocation(s"$fileName-$i.parquet")
    val out = table.io().newOutputFile(path)
    val appender = new GenericAppenderFactory(table.schema(), spec).newAppender(out, FileFormat.PARQUET)
    try recs.foreach(appender.add) finally appender.close()
    val builder = DataFiles.builder(spec).withInputFile(out.toInputFile()).withMetrics(appender.metrics()).withFormat(FileFormat.PARQUET)
    if (spec.isPartitioned) builder.withPartition(key)
    append.appendFile(builder.build())
  }
  append.commit()
}

def record(schema: Schema, values: (String, Any)*): Record = {
  val rec = GenericRecord.create(schema)
  values.foreach { case (k, v) => rec.setField(k, v) }
  rec
}

// --- nanosecond timestamps, unpartitioned (Spark cannot read or write timestamp_ns) ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "ts_ns", Types.TimestampNanoType.withoutZone()),
    Types.NestedField.optional(3, "tstz_ns", Types.TimestampNanoType.withZone()),
    Types.NestedField.optional(4, "ts_us", Types.TimestampType.withoutZone())
  )
  val table = recreate("test_v3_ns_timestamps", schema)
  appendRecords(
    table,
    Seq(
      record(
        schema,
        "id" -> Integer.valueOf(1),
        "ts_ns" -> LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123456789),
        "tstz_ns" -> OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 123456789, ZoneOffset.UTC),
        "ts_us" -> LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123456000)
      ),
      record(
        schema,
        "id" -> Integer.valueOf(2),
        "ts_ns" -> LocalDateTime.of(2024, 2, 2, 0, 0, 0, 1),
        "tstz_ns" -> OffsetDateTime.of(2024, 2, 2, 0, 0, 0, 1, ZoneOffset.UTC),
        "ts_us" -> LocalDateTime.of(2024, 2, 2, 0, 0, 0, 0)
      ),
      record(schema, "id" -> Integer.valueOf(3))
    ),
    "ns"
  )
  println("PROVISIONED test_v3_ns_timestamps")
}

// --- nanosecond timestamps partitioned by year(ts), month(tstz), bucket(tstz, 4) ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone()),
    Types.NestedField.optional(3, "tstz", Types.TimestampNanoType.withZone())
  )
  val spec = PartitionSpec.builderFor(schema).year("ts").month("tstz").bucket("tstz", 4).build()
  val table = recreate("test_v3_ns_partitions", schema, spec)
  appendRecords(
    table,
    Seq(
      record(schema, "id" -> Integer.valueOf(1), "ts" -> LocalDateTime.of(2023, 12, 31, 23, 59, 59, 999999999), "tstz" -> OffsetDateTime.of(2023, 12, 31, 23, 59, 59, 999999999, ZoneOffset.UTC)),
      record(schema, "id" -> Integer.valueOf(2), "ts" -> LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1), "tstz" -> OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 1, ZoneOffset.UTC)),
      record(schema, "id" -> Integer.valueOf(3), "ts" -> LocalDateTime.of(2024, 5, 5, 5, 5, 5, 0), "tstz" -> OffsetDateTime.of(2024, 5, 5, 5, 5, 5, 0, ZoneOffset.UTC))
    ),
    "nsp"
  )
  println("PROVISIONED test_v3_ns_partitions")
}

// --- geometry / geography: schema only. Iceberg 1.11's generic Parquet writer does not support geo types. ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()),
    Types.NestedField.optional(3, "geom_srid", Types.GeometryType.of("srid:3857")),
    Types.NestedField.optional(4, "geog", Types.GeographyType.crs84()),
    Types.NestedField.optional(5, "geog_v", Types.GeographyType.of("srid:4326", org.apache.iceberg.types.EdgeAlgorithm.VINCENTY))
  )
  recreate("test_v3_geo", schema)
  println("PROVISIONED test_v3_geo")
}

// --- column defaults (Spark SQL refuses ADD COLUMN ... DEFAULT); rows inserted through Spark ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get())
  )
  val table = recreate("test_v3_defaults", schema)
  spark.sql("INSERT INTO rest.default.test_v3_defaults VALUES (1, 'one'), (2, 'two')")
  table.refresh()
  table
    .updateSchema()
    .addColumn("color", Types.StringType.get(), "doc", Literal.of("blue"))
    .addColumn("qty", Types.IntegerType.get(), "doc", Literal.of(42))
    .addColumn("ratio", Types.DoubleType.get(), "doc", Literal.of(1.5d))
    .addColumn("d", Types.DateType.get(), "doc", Literal.of("2024-03-04").to(Types.DateType.get()))
    .addColumn("ts", Types.TimestampType.withZone(), "doc", Literal.of("2024-03-04T05:06:07+00:00").to(Types.TimestampType.withZone()))
    .addColumn("dec", Types.DecimalType.of(10, 2), "doc", Literal.of(new BigDecimal("12.34")))
    .addColumn("b", Types.BooleanType.get(), "doc", Literal.of(true))
    .addColumn("bin", Types.BinaryType.get(), "doc", Literal.of(ByteBuffer.wrap(Array[Byte](1, 2))))
    .addColumn("u", Types.UUIDType.get(), "doc", Literal.of("f79c3e09-677c-4bbd-a479-3f349cb785e7").to(Types.UUIDType.get()))
    .addRequiredColumn("req", Types.LongType.get(), "doc", Literal.of(7L))
    .commit()
  table.refresh()
  // write-default changes, initial-default stays "blue"
  table.updateSchema().updateColumnDefault("color", Literal.of("green")).commit()
  println("PROVISIONED test_v3_defaults")
}

// --- nested struct default ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(
      2,
      "s",
      Types.StructType.of(Types.NestedField.optional(3, "a", Types.IntegerType.get()), Types.NestedField.optional(4, "b", Types.StringType.get()))
    )
  )
  val table = recreate("test_v3_nested_defaults", schema)
  spark.sql("INSERT INTO rest.default.test_v3_nested_defaults VALUES (1, named_struct('a', 1, 'b', 'x'))")
  table.refresh()
  table.updateSchema().addColumn("s", "c", Types.IntegerType.get(), "doc", Literal.of(99)).commit()
  println("PROVISIONED test_v3_nested_defaults")
}

// --- unknown type column added after data exists ---
{
  val schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
  val table = recreate("test_v3_unknown", schema)
  spark.sql("INSERT INTO rest.default.test_v3_unknown VALUES (1), (2)")
  table.refresh()
  table.updateSchema().addColumn("unk", Types.UnknownType.get()).commit()
  println("PROVISIONED test_v3_unknown")
}

// --- equality deletes (Spark SQL only writes position deletes and deletion vectors) ---
{
  val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "value", Types.DoubleType.get())
  )
  val table = recreate("test_v3_equality_deletes", schema)
  appendRecords(
    table,
    Seq(
      record(schema, "id" -> Integer.valueOf(1), "name" -> "a", "value" -> java.lang.Double.valueOf(1.0)),
      record(schema, "id" -> Integer.valueOf(2), "name" -> "b", "value" -> java.lang.Double.valueOf(2.0)),
      record(schema, "id" -> Integer.valueOf(3), "name" -> "c", "value" -> java.lang.Double.valueOf(3.0)),
      record(schema, "id" -> Integer.valueOf(4), "value" -> java.lang.Double.valueOf(4.0)),
      record(schema, "id" -> Integer.valueOf(5), "name" -> "e", "value" -> java.lang.Double.valueOf(5.0))
    ),
    "eq-data"
  )

  def writeEqDeletes(fieldIds: Array[Int], deleteSchema: Schema, rows: Seq[Record], fileName: String): org.apache.iceberg.DeleteFile = {
    val factory = new GenericAppenderFactory(table.schema(), table.spec(), fieldIds, deleteSchema, null)
    val out = table.io().newOutputFile(table.locationProvider().newDataLocation(s"$fileName.parquet"))
    val writer = factory.newEqDeleteWriter(org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null)
    try rows.foreach(writer.write) finally writer.close()
    writer.toDeleteFile()
  }

  // delete rows by id, and by name where a null delete value matches a null data value
  val idSchema = table.schema().select("id")
  val nameSchema = table.schema().select("name")
  val byId = writeEqDeletes(Array(1), idSchema, Seq(record(idSchema, "id" -> Integer.valueOf(2)), record(idSchema, "id" -> Integer.valueOf(5))), "eq-delete-id")
  val byName = writeEqDeletes(Array(2), nameSchema, Seq(record(nameSchema)), "eq-delete-name")
  table.newRowDelta().addDeletes(byId).addDeletes(byName).commit()

  // rows added after the deletes are not affected by them
  appendRecords(table, Seq(record(schema, "id" -> Integer.valueOf(2), "name" -> "b2", "value" -> java.lang.Double.valueOf(20.0))), "eq-data-after")
  println("PROVISIONED test_v3_equality_deletes")
}

println("PROVISION_V3_DONE")
System.exit(0)
