package com.etl.util

import com.etl.config.{CredentialVault, ExtractConfig, LoadConfig}
import org.slf4j.LoggerFactory

/**
 * Helper utilities for retrieving credentials from vault or config.
 * Provides backward compatibility with plain-text passwords while
 * encouraging use of secure credential vault.
 */
object CredentialHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Get password from vault (preferred) or fall back to config.
   *
   * @param config Extract configuration
   * @param vault Credential vault
   * @param paramName Password parameter name in connectionParams (default: "password")
   * @return Password string
   * @throws IllegalArgumentException if neither credentialId nor password found
   */
  def getPasswordFromExtractConfig(
    config: ExtractConfig,
    vault: CredentialVault,
    paramName: String = "password"
  ): String = {
    config.credentialId match {
      case Some(credId) =>
        // Preferred: Use secure vault
        logger.debug(s"Retrieving credential from vault: $credId")
        try {
          vault.getCredential(credId)
        } catch {
          case e: IllegalArgumentException =>
            logger.warn(s"Credential not found in vault: $credId. Falling back to config.")
            // Fall back to plain-text if vault lookup fails
            config.connectionParams.get(paramName).getOrElse(
              throw new IllegalArgumentException(
                s"Credential '$credId' not found in vault and no '$paramName' in config"
              )
            )
        }

      case None =>
        // Backward compatibility: Use plain-text from config
        config.connectionParams.get(paramName) match {
          case Some(pwd) =>
            logger.warn(
              s"Using plain-text password from config. " +
                "Consider using credentialId with encrypted vault for security."
            )
            pwd
          case None =>
            throw new IllegalArgumentException(
              s"Neither credentialId nor '$paramName' provided. " +
                s"Provide credentialId (secure) or $paramName (plain-text) in config."
            )
        }
    }
  }

  /**
   * Get password from vault (preferred) or fall back to config for loaders.
   *
   * @param config Load configuration
   * @param vault Credential vault
   * @param paramName Password parameter name in connectionParams (default: "password")
   * @return Password string
   * @throws IllegalArgumentException if neither credentialId nor password found
   */
  def getPasswordFromLoadConfig(
    config: LoadConfig,
    vault: CredentialVault,
    paramName: String = "password"
  ): String = {
    config.credentialId match {
      case Some(credId) =>
        // Preferred: Use secure vault
        logger.debug(s"Retrieving credential from vault: $credId")
        try {
          vault.getCredential(credId)
        } catch {
          case e: IllegalArgumentException =>
            logger.warn(s"Credential not found in vault: $credId. Falling back to config.")
            // Fall back to plain-text if vault lookup fails
            config.connectionParams.get(paramName).getOrElse(
              throw new IllegalArgumentException(
                s"Credential '$credId' not found in vault and no '$paramName' in config"
              )
            )
        }

      case None =>
        // Backward compatibility: Use plain-text from config
        config.connectionParams.get(paramName) match {
          case Some(pwd) =>
            logger.warn(
              s"Using plain-text password from config. " +
                "Consider using credentialId with encrypted vault for security."
            )
            pwd
          case None =>
            throw new IllegalArgumentException(
              s"Neither credentialId nor '$paramName' provided. " +
                s"Provide credentialId (secure) or $paramName (plain-text) in config."
            )
        }
    }
  }

  /**
   * Get AWS credentials from vault or config.
   *
   * @param config Extract or Load configuration connectionParams
   * @param vault Credential vault
   * @return Tuple of (accessKey, secretKey)
   * @throws IllegalArgumentException if credentials not found
   */
  def getS3Credentials(
    connectionParams: Map[String, String],
    credentialId: Option[String],
    vault: CredentialVault
  ): (String, String) = {
    credentialId match {
      case Some(credId) =>
        // Vault should have two entries: {credId}.access and {credId}.secret
        logger.debug(s"Retrieving S3 credentials from vault: $credId")
        try {
          val accessKey = vault.getCredential(s"$credId.access")
          val secretKey = vault.getCredential(s"$credId.secret")
          (accessKey, secretKey)
        } catch {
          case e: IllegalArgumentException =>
            logger.warn(s"S3 credentials not found in vault: $credId. Falling back to config.")
            getS3CredentialsFromParams(connectionParams)
        }

      case None =>
        logger.warn(
          "Using plain-text S3 credentials from config. " +
            "Consider using credentialId with encrypted vault."
        )
        getS3CredentialsFromParams(connectionParams)
    }
  }

  /**
   * Extract S3 credentials from connection parameters.
   */
  private def getS3CredentialsFromParams(params: Map[String, String]): (String, String) = {
    val accessKey = params.get("fs.s3a.access.key").orElse(params.get("s3.access.key")).getOrElse(
      throw new IllegalArgumentException(
        "S3 access key not found. Provide credentialId or fs.s3a.access.key in config."
      )
    )
    val secretKey = params.get("fs.s3a.secret.key").orElse(params.get("s3.secret.key")).getOrElse(
      throw new IllegalArgumentException(
        "S3 secret key not found. Provide credentialId or fs.s3a.secret.key in config."
      )
    )
    (accessKey, secretKey)
  }

  /**
   * Get Kafka SASL credentials from vault or config.
   *
   * @param connectionParams Kafka connection parameters
   * @param credentialId Optional credential ID
   * @param vault Credential vault
   * @return SASL JAAS config string
   */
  def getKafkaSaslConfig(
    connectionParams: Map[String, String],
    credentialId: Option[String],
    vault: CredentialVault
  ): Option[String] = {
    // If SASL config already in params, return it
    connectionParams.get("kafka.sasl.jaas.config") match {
      case Some(jaasConfig) =>
        logger.warn("Using plain-text SASL config from params. Use credentialId for security.")
        Some(jaasConfig)

      case None =>
        // Try to build from vault
        credentialId.flatMap { credId =>
          try {
            val username = vault.getCredential(s"$credId.username")
            val password = vault.getCredential(s"$credId.password")
            val mechanism = connectionParams.getOrElse("kafka.sasl.mechanism", "PLAIN")

            val jaasConfig = mechanism match {
              case "PLAIN" =>
                s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$username" password="$password";"""
              case "SCRAM-SHA-256" | "SCRAM-SHA-512" =>
                s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""
              case _ =>
                throw new IllegalArgumentException(s"Unsupported SASL mechanism: $mechanism")
            }

            logger.debug(s"Built SASL config from vault credentials: $credId")
            Some(jaasConfig)
          } catch {
            case e: IllegalArgumentException =>
              logger.debug(s"Kafka SASL credentials not found in vault: $credId")
              None
          }
        }
    }
  }
}
