package com.etl.contract.schemas

import com.etl.schema.SchemaRegistry
import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
 * Contract test for user-event.avsc schema.
 * Verifies that the schema matches expected structure and field definitions.
 */
class UserEventSchemaSpec extends AnyFlatSpec with Matchers {

  "UserEvent schema" should "be loaded from SchemaRegistry" in {
    // When loading schema
    val schema = SchemaRegistry.getSchema("user-event")

    // Then should be a valid record schema
    schema.getType shouldBe Schema.Type.RECORD
    schema.getName shouldBe "UserEvent"
    schema.getNamespace shouldBe "com.etl.schema"
  }

  it should "have event_id field as string" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val eventIdField = schema.getField("event_id")

    eventIdField should not be null
    eventIdField.schema().getType shouldBe Schema.Type.STRING
    eventIdField.doc() should include("Unique event identifier")
  }

  it should "have user_id field as string" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val userIdField = schema.getField("user_id")

    userIdField should not be null
    userIdField.schema().getType shouldBe Schema.Type.STRING
    userIdField.doc() should include("User identifier")
  }

  it should "have event_type field as enum with correct symbols" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val eventTypeField = schema.getField("event_type")

    eventTypeField should not be null
    eventTypeField.schema().getType shouldBe Schema.Type.ENUM
    eventTypeField.schema().getName shouldBe "EventType"

    val symbols = eventTypeField.schema().getEnumSymbols.asScala.toSet
    symbols should contain allOf ("PAGE_VIEW", "BUTTON_CLICK", "FORM_SUBMIT", "API_CALL")
    symbols should have size 4
  }

  it should "have timestamp field as long with logical type" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val timestampField = schema.getField("timestamp")

    timestampField should not be null
    timestampField.schema().getType shouldBe Schema.Type.LONG
    timestampField.schema().getLogicalType.getName shouldBe "timestamp-millis"
  }

  it should "have properties field as nullable map of strings" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val propertiesField = schema.getField("properties")

    propertiesField should not be null
    propertiesField.schema().getType shouldBe Schema.Type.UNION

    // Union should contain null and map
    val unionTypes = propertiesField.schema().getTypes.asScala
    unionTypes should have size 2

    val hasNull = unionTypes.exists(_.getType == Schema.Type.NULL)
    val mapType = unionTypes.find(_.getType == Schema.Type.MAP)

    hasNull shouldBe true
    mapType should not be empty
    mapType.get.getValueType.getType shouldBe Schema.Type.STRING
  }

  it should "have amount field as nullable double" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val amountField = schema.getField("amount")

    amountField should not be null
    amountField.schema().getType shouldBe Schema.Type.UNION

    // Union should contain null and double
    val unionTypes = amountField.schema().getTypes.asScala
    unionTypes should have size 2

    val hasNull = unionTypes.exists(_.getType == Schema.Type.NULL)
    val hasDouble = unionTypes.exists(_.getType == Schema.Type.DOUBLE)

    hasNull shouldBe true
    hasDouble shouldBe true
  }

  it should "have all required fields present" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSet

    fieldNames should contain allOf (
      "event_id",
      "user_id",
      "event_type",
      "timestamp",
      "properties",
      "amount"
    )
    fieldNames should have size 6
  }

  it should "have correct field order" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSeq

    fieldNames shouldBe Seq(
      "event_id",
      "user_id",
      "event_type",
      "timestamp",
      "properties",
      "amount"
    )
  }

  it should "have properties field with default null" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val propertiesField = schema.getField("properties")

    propertiesField.hasDefaultValue shouldBe true
    propertiesField.defaultVal() shouldBe null
  }

  it should "have amount field with default null" in {
    val schema = SchemaRegistry.getSchema("user-event")
    val amountField = schema.getField("amount")

    amountField.hasDefaultValue shouldBe true
    amountField.defaultVal() shouldBe null
  }
}
