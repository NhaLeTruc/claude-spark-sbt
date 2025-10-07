package com.etl.contract.schemas

import com.etl.schema.SchemaRegistry
import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
 * Contract test for transaction.avsc schema.
 * Verifies that the schema matches expected structure and field definitions.
 */
class TransactionSchemaSpec extends AnyFlatSpec with Matchers {

  "Transaction schema" should "be loaded from SchemaRegistry" in {
    // When loading schema
    val schema = SchemaRegistry.getSchema("transaction")

    // Then should be a valid record schema
    schema.getType shouldBe Schema.Type.RECORD
    schema.getName shouldBe "Transaction"
    schema.getNamespace shouldBe "com.etl.schema"
  }

  it should "have transaction_id field as string" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("transaction_id")

    field should not be null
    field.schema().getType shouldBe Schema.Type.STRING
    field.doc() should include("Unique transaction identifier")
  }

  it should "have user_id field as string" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("user_id")

    field should not be null
    field.schema().getType shouldBe Schema.Type.STRING
    field.doc() should include("User identifier")
  }

  it should "have amount field as double" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("amount")

    field should not be null
    field.schema().getType shouldBe Schema.Type.DOUBLE
    field.doc() should include("Transaction amount")
  }

  it should "have currency field as string" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("currency")

    field should not be null
    field.schema().getType shouldBe Schema.Type.STRING
    field.doc() should include("ISO currency code")
  }

  it should "have transaction_time field as long with logical type" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("transaction_time")

    field should not be null
    field.schema().getType shouldBe Schema.Type.LONG
    field.schema().getLogicalType.getName shouldBe "timestamp-millis"
  }

  it should "have status field as enum with correct symbols" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("status")

    field should not be null
    field.schema().getType shouldBe Schema.Type.ENUM
    field.schema().getName shouldBe "TransactionStatus"

    val symbols = field.schema().getEnumSymbols.asScala.toSet
    symbols should contain allOf ("PENDING", "COMPLETED", "FAILED", "REFUNDED")
    symbols should have size 4
  }

  it should "have metadata field as nullable record" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("metadata")

    field should not be null
    field.schema().getType shouldBe Schema.Type.UNION

    // Union should contain null and record
    val unionTypes = field.schema().getTypes.asScala
    unionTypes should have size 2

    val hasNull = unionTypes.exists(_.getType == Schema.Type.NULL)
    val recordType = unionTypes.find(_.getType == Schema.Type.RECORD)

    hasNull shouldBe true
    recordType should not be empty
    recordType.get.getName shouldBe "TransactionMetadata"
  }

  it should "have metadata record with correct nested fields" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val metadataField = schema.getField("metadata")

    val recordType = metadataField.schema().getTypes.asScala
      .find(_.getType == Schema.Type.RECORD)
      .get

    val fieldNames = recordType.getFields.asScala.map(_.name()).toSet
    fieldNames should contain allOf ("ip_address", "device_id", "merchant_id")
    fieldNames should have size 3

    // All nested fields should be nullable strings
    recordType.getFields.asScala.foreach { field =>
      field.schema().getType shouldBe Schema.Type.UNION

      val types = field.schema().getTypes.asScala
      types should have size 2
      types.exists(_.getType == Schema.Type.NULL) shouldBe true
      types.exists(_.getType == Schema.Type.STRING) shouldBe true

      field.hasDefaultValue shouldBe true
      field.defaultVal() shouldBe null
    }
  }

  it should "have all required fields present" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSet

    fieldNames should contain allOf (
      "transaction_id",
      "user_id",
      "amount",
      "currency",
      "transaction_time",
      "status",
      "metadata"
    )
    fieldNames should have size 7
  }

  it should "have correct field order" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val fieldNames = schema.getFields.asScala.map(_.name()).toSeq

    fieldNames shouldBe Seq(
      "transaction_id",
      "user_id",
      "amount",
      "currency",
      "transaction_time",
      "status",
      "metadata"
    )
  }

  it should "have metadata field with default null" in {
    val schema = SchemaRegistry.getSchema("transaction")
    val field = schema.getField("metadata")

    field.hasDefaultValue shouldBe true
    field.defaultVal() shouldBe null
  }

  it should "have required fields without defaults" in {
    val schema = SchemaRegistry.getSchema("transaction")

    val requiredFields = Seq("transaction_id", "user_id", "amount", "currency", "transaction_time", "status")

    requiredFields.foreach { fieldName =>
      val field = schema.getField(fieldName)
      field should not be null
      // Required fields should not be unions (not nullable)
      if (field.schema().getType != Schema.Type.UNION) {
        field.schema().getType should not be Schema.Type.NULL
      }
    }
  }
}
