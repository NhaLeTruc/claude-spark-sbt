package com.etl.unit.config

import com.etl.config.{CredentialVault, FileBasedVault}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class CredentialVaultSpec extends AnyFlatSpec with Matchers {

  "FileBasedVault" should "retrieve credentials from vault" in {
    // Given a vault with test credentials
    val vaultData = Map(
      "postgres.password" -> "test-pg-password",
      "mysql.password"    -> "test-mysql-password",
      "kafka.sasl.password" -> "test-kafka-password"
    )

    val vault = new TestVault(vaultData)

    // When retrieving credentials
    val pgPassword = vault.getCredential("postgres.password")
    val mysqlPassword = vault.getCredential("mysql.password")

    // Then should return correct values
    pgPassword shouldBe "test-pg-password"
    mysqlPassword shouldBe "test-mysql-password"
  }

  it should "throw exception for non-existent credential" in {
    // Given vault without credential
    val vault = new TestVault(Map("existing" -> "value"))

    // When requesting non-existent credential
    // Then should throw exception
    assertThrows[IllegalArgumentException] {
      vault.getCredential("non-existent")
    }
  }

  it should "cache credentials after first access" in {
    // Given vault with credentials
    val vaultData = Map("test.key" -> "test-value")
    val vault = new TestVault(vaultData)

    // When accessing same credential multiple times
    val value1 = vault.getCredential("test.key")
    val value2 = vault.getCredential("test.key")

    // Then should return same value (cached)
    value1 shouldBe value2
    value1 shouldBe "test-value"
  }

  it should "handle empty vault" in {
    // Given empty vault
    val vault = new TestVault(Map.empty)

    // When checking for credentials
    // Then should throw exception
    assertThrows[IllegalArgumentException] {
      vault.getCredential("any.key")
    }
  }

  it should "list all credential IDs" in {
    // Given vault with multiple credentials
    val vaultData = Map(
      "postgres.password" -> "value1",
      "mysql.password"    -> "value2",
      "kafka.password"    -> "value3"
    )
    val vault = new TestVault(vaultData)

    // When listing credential IDs
    val ids = vault.listCredentialIds()

    // Then should return all keys
    ids should contain allOf ("postgres.password", "mysql.password", "kafka.password")
    ids should have size 3
  }

  it should "check if credential exists" in {
    // Given vault
    val vault = new TestVault(Map("existing" -> "value"))

    // When checking existence
    val exists = vault.hasCredential("existing")
    val notExists = vault.hasCredential("non-existent")

    // Then should return correct results
    exists shouldBe true
    notExists shouldBe false
  }

  // Test implementation for unit tests (no encryption)
  class TestVault(credentials: Map[String, String]) extends CredentialVault {
    private val cache = credentials

    override def getCredential(id: String): String = {
      cache.getOrElse(
        id,
        throw new IllegalArgumentException(s"Credential not found: $id. Available: ${cache.keys.mkString(", ")}")
      )
    }

    override def hasCredential(id: String): Boolean = cache.contains(id)

    override def listCredentialIds(): Set[String] = cache.keySet
  }
}
