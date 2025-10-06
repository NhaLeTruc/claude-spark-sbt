package com.etl.unit.schema

import com.etl.schema.SchemaRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaRegistrySpec extends AnyFlatSpec with Matchers {

  "SchemaRegistry" should "load all Avro schemas from resources" in {
    // When loading schemas
    val schemas = SchemaRegistry.listSchemas()

    // Then should have at least 3 schemas
    schemas should not be empty
    schemas should contain allOf ("user-event", "transaction", "user-summary")
  }

  it should "retrieve schema by name" in {
    // When getting a known schema
    val schema = SchemaRegistry.getSchema("user-event")

    // Then schema should be valid and have expected fields
    schema should not be null
    schema.getName shouldBe "UserEvent"
    schema.getNamespace shouldBe "com.etl.schema"

    // Verify key fields exist
    val fieldNames = schema.getFields.toArray.map(_.asInstanceOf[org.apache.avro.Schema.Field].name)
    fieldNames should contain allOf ("event_id", "user_id", "event_type", "timestamp")
  }

  it should "throw exception for non-existent schema" in {
    // When requesting non-existent schema
    // Then should throw IllegalArgumentException
    assertThrows[IllegalArgumentException] {
      SchemaRegistry.getSchema("non-existent-schema")
    }
  }

  it should "handle transaction schema correctly" in {
    // When getting transaction schema
    val schema = SchemaRegistry.getSchema("transaction")

    // Then should have correct structure
    schema.getName shouldBe "Transaction"
    val fieldNames = schema.getFields.toArray.map(_.asInstanceOf[org.apache.avro.Schema.Field].name)
    fieldNames should contain allOf (
      "transaction_id",
      "user_id",
      "amount",
      "currency",
      "transaction_time",
      "status"
    )
  }

  it should "handle user-summary schema correctly" in {
    // When getting user-summary schema
    val schema = SchemaRegistry.getSchema("user-summary")

    // Then should have aggregation fields
    schema.getName shouldBe "UserSummary"
    val fieldNames = schema.getFields.toArray.map(_.asInstanceOf[org.apache.avro.Schema.Field].name)
    fieldNames should contain allOf (
      "user_id",
      "total_events",
      "total_amount",
      "last_event_time",
      "updated_at"
    )
  }
}
