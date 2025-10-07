package com.etl.contract.schemas

import com.etl.schema.SchemaRegistry
import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
 * Contract test for user-summary.avsc schema.
 * Verifies that the aggregation output schema matches expected structure.
 */
class UserSummarySchemaSpec extends AnyFlatSpec with Matchers {

  "UserSummary schema" should "be loaded from SchemaRegistry" in {
    // When loading schema
    val schema = SchemaRegistry.getSchema("user-summary")

    // Then should be a valid record schema
    schema.getType shouldBe Schema.Type.RECORD
    schema.getName shouldBe "UserSummary"
    schema.getNamespace shouldBe "com.etl.schema"
    schema.getDoc should include("aggregated user metrics")
  }

  it should "have user_id field as string" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("user_id")

    field should not be null
    field.schema().getType shouldBe Schema.Type.STRING
    field.doc() should include("User identifier")
    field.doc() should include("primary key")
  }

  it should "have total_events field as long" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("total_events")

    field should not be null
    field.schema().getType shouldBe Schema.Type.LONG
    field.doc() should include("Total count of user events")
  }

  it should "have total_amount field as double" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("total_amount")

    field should not be null
    field.schema().getType shouldBe Schema.Type.DOUBLE
    field.doc() should include("Sum of transaction amounts")
  }

  it should "have last_event_time field as long with logical type" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("last_event_time")

    field should not be null
    field.schema().getType shouldBe Schema.Type.LONG
    field.schema().getLogicalType.getName shouldBe "timestamp-millis"
    field.doc() should include("most recent event")
  }

  it should "have event_type_counts field as map of longs" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("event_type_counts")

    field should not be null
    field.schema().getType shouldBe Schema.Type.MAP
    field.schema().getValueType.getType shouldBe Schema.Type.LONG
    field.doc() should include("Count of events by type")
  }

  it should "have updated_at field as long with logical type" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val field = schema.getField("updated_at")

    field should not be null
    field.schema().getType shouldBe Schema.Type.LONG
    field.schema().getLogicalType.getName shouldBe "timestamp-millis"
    field.doc() should include("Pipeline execution timestamp")
  }

  it should "have all required fields present" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSet

    fieldNames should contain allOf (
      "user_id",
      "total_events",
      "total_amount",
      "last_event_time",
      "event_type_counts",
      "updated_at"
    )
    fieldNames should have size 6
  }

  it should "have correct field order" in {
    val schema = SchemaRegistry.getSchema("user-summary")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSeq

    fieldNames shouldBe Seq(
      "user_id",
      "total_events",
      "total_amount",
      "last_event_time",
      "event_type_counts",
      "updated_at"
    )
  }

  it should "have all fields as non-nullable (required)" in {
    val schema = SchemaRegistry.getSchema("user-summary")

    schema.getFields.asScala.foreach { field =>
      // None of the fields should be unions (all required)
      field.schema().getType should not be Schema.Type.UNION
    }
  }

  it should "have numeric aggregation fields with appropriate types" in {
    val schema = SchemaRegistry.getSchema("user-summary")

    // total_events should be long (count)
    schema.getField("total_events").schema().getType shouldBe Schema.Type.LONG

    // total_amount should be double (sum of doubles)
    schema.getField("total_amount").schema().getType shouldBe Schema.Type.DOUBLE
  }

  it should "have timestamp fields with correct logical types" in {
    val schema = SchemaRegistry.getSchema("user-summary")

    val timestampFields = Seq("last_event_time", "updated_at")

    timestampFields.foreach { fieldName =>
      val field = schema.getField(fieldName)
      field.schema().getType shouldBe Schema.Type.LONG
      field.schema().getLogicalType should not be null
      field.schema().getLogicalType.getName shouldBe "timestamp-millis"
    }
  }
}
