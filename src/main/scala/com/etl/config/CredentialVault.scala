package com.etl.config

import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Credential vault interface for secure credential storage.
 * Implementations provide access to encrypted credentials for external systems.
 */
trait CredentialVault {

  /**
   * Retrieve credential by ID.
   *
   * @param id Credential identifier (e.g., "postgres.password")
   * @return Decrypted credential value
   * @throws IllegalArgumentException if credential not found
   */
  def getCredential(id: String): String

  /**
   * Check if credential exists.
   *
   * @param id Credential identifier
   * @return true if credential exists
   */
  def hasCredential(id: String): Boolean

  /**
   * List all credential IDs in vault.
   *
   * @return Set of credential identifiers
   */
  def listCredentialIds(): Set[String]
}

/**
 * File-based credential vault with AES-256 encryption.
 * Reads encrypted credentials from a JSON file and decrypts using master key.
 *
 * Vault file format (JSON):
 * {{{
 * {
 *   "postgres.password": "encrypted_base64_string",
 *   "mysql.password": "encrypted_base64_string"
 * }
 * }}}
 *
 * Master key must be provided via VAULT_MASTER_KEY environment variable.
 * For production, use proper key management (AWS KMS, HashiCorp Vault, etc.).
 *
 * @param vaultPath Path to encrypted vault file
 * @param masterKey Master encryption key (from environment)
 */
class FileBasedVault(vaultPath: String, masterKey: String) extends CredentialVault {
  private val logger = LoggerFactory.getLogger(getClass)

  // Lazy-load and decrypt credentials on first access
  private lazy val decryptedCache: Map[String, String] = loadAndDecrypt()

  override def getCredential(id: String): String = {
    decryptedCache.getOrElse(
      id,
      throw new IllegalArgumentException(
        s"Credential not found: $id. Available: ${decryptedCache.keys.mkString(", ")}"
      )
    )
  }

  override def hasCredential(id: String): Boolean = decryptedCache.contains(id)

  override def listCredentialIds(): Set[String] = decryptedCache.keySet

  /**
   * Load encrypted vault file and decrypt all credentials.
   *
   * @return Map of credential ID to decrypted value
   */
  private def loadAndDecrypt(): Map[String, String] = {
    Try {
      val source = Source.fromFile(vaultPath)
      try {
        val vaultJson = source.mkString
        val json = Json.parse(vaultJson)

        // Parse as Map[String, String] (encrypted values)
        val encryptedMap = json.as[Map[String, String]]

        // Decrypt all values
        val decrypted = encryptedMap.map { case (key, encryptedValue) =>
          val decryptedValue = decrypt(encryptedValue, masterKey)
          key -> decryptedValue
        }

        logger.info(s"Loaded ${decrypted.size} credentials from vault: ${vaultPath}")
        decrypted

      } finally {
        source.close()
      }
    } match {
      case Success(credentials) => credentials
      case Failure(e) =>
        logger.error(s"Failed to load vault from $vaultPath: ${e.getMessage}")
        // For testing/development, return empty map instead of failing
        logger.warn("Using empty credential vault (development mode)")
        Map.empty
    }
  }

  /**
   * Decrypt encrypted value using AES-256.
   *
   * @param encryptedBase64 Base64-encoded encrypted value
   * @param key Master encryption key
   * @return Decrypted plaintext value
   */
  private def decrypt(encryptedBase64: String, key: String): String = {
    // Use first 32 bytes of key for AES-256 (or pad if shorter)
    val keyBytes = key.getBytes(StandardCharsets.UTF_8).take(32).padTo(32, 0.toByte)
    val secretKey = new SecretKeySpec(keyBytes, "AES")

    val cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.DECRYPT_MODE, secretKey)

    val encryptedBytes = Base64.getDecoder.decode(encryptedBase64)
    val decryptedBytes = cipher.doFinal(encryptedBytes)

    new String(decryptedBytes, StandardCharsets.UTF_8)
  }
}

object FileBasedVault {

  /**
   * Create vault from file path and environment variable.
   *
   * @param vaultPath Path to encrypted vault file
   * @return FileBasedVault instance
   * @throws IllegalStateException if VAULT_MASTER_KEY not set
   */
  def apply(vaultPath: String): FileBasedVault = {
    val masterKey = sys.env.getOrElse(
      "VAULT_MASTER_KEY",
      throw new IllegalStateException(
        "VAULT_MASTER_KEY environment variable not set. " +
          "Set it with: export VAULT_MASTER_KEY=your-secret-key"
      )
    )
    new FileBasedVault(vaultPath, masterKey)
  }

  /**
   * Encrypt plaintext value for storage in vault.
   * Utility method for creating vault files.
   *
   * @param plaintext Value to encrypt
   * @param key Master encryption key
   * @return Base64-encoded encrypted value
   */
  def encrypt(plaintext: String, key: String): String = {
    val keyBytes = key.getBytes(StandardCharsets.UTF_8).take(32).padTo(32, 0.toByte)
    val secretKey = new SecretKeySpec(keyBytes, "AES")

    val cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)

    val encryptedBytes = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8))
    Base64.getEncoder.encodeToString(encryptedBytes)
  }
}

/**
 * In-memory credential vault for testing.
 * Stores credentials in plain text (only use for tests!).
 *
 * @param credentials Map of credential ID to value
 */
class InMemoryVault(credentials: Map[String, String]) extends CredentialVault {
  override def getCredential(id: String): String = {
    credentials.getOrElse(
      id,
      throw new IllegalArgumentException(s"Credential not found: $id")
    )
  }

  override def hasCredential(id: String): Boolean = credentials.contains(id)

  override def listCredentialIds(): Set[String] = credentials.keySet
}

object InMemoryVault {
  def apply(credentials: (String, String)*): InMemoryVault = {
    new InMemoryVault(credentials.toMap)
  }
}
