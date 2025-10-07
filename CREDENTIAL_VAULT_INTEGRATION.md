# CredentialVault Integration - Phase 1 Implementation

**Status**: IN PROGRESS (60% Complete)
**Started**: 2025-10-07
**Estimated Completion**: 4 hours remaining

---

## Overview

Implementing secure credential management across all extractors and loaders to eliminate plain-text passwords in configuration files.

**Security Benefits**:
- ✅ Encrypted credential storage using AES-256
- ✅ Credentials retrieved at runtime from secure vault
- ✅ No passwords in JSON configs, logs, or version control
- ✅ Backward compatible with existing configs (with warnings)

---

## Completed Work (60% - 4.8 hours)

### 1. ✅ ExecutionContext Enhanced
**File**: `src/main/scala/com/etl/core/ExecutionContext.scala`

**Changes**:
```scala
case class ExecutionContext(
  spark: SparkSession,
  config: PipelineConfig,
  vault: CredentialVault,  // ✅ Added
  var metrics: ExecutionMetrics,
  traceId: String = UUID.randomUUID().toString
)

// Updated factory method
def create(spark: SparkSession, config: PipelineConfig, vault: CredentialVault): ExecutionContext
```

**Impact**: All pipeline executions now have access to credential vault

---

### 2. ✅ Credential Helper Utility Created
**File**: `src/main/scala/com/etl/util/CredentialHelper.scala` (NEW - 205 lines)

**Functions**:
- `getPasswordFromExtractConfig()` - Get JDBC/database passwords
- `getPasswordFromLoadConfig()` - Get passwords for loaders
- `getS3Credentials()` - Get AWS access/secret keys
- `getKafkaSaslConfig()` - Get Kafka SASL credentials

**Features**:
- Tries vault first (secure)
- Falls back to config (backward compatible)
- Logs warnings for plain-text usage
- Supports multiple credential formats

**Example Usage**:
```scala
// In extractor/loader
val password = CredentialHelper.getPasswordFromExtractConfig(config, vault)

// S3 credentials
val (accessKey, secretKey) = CredentialHelper.getS3Credentials(
  config.connectionParams,
  config.credentialId,
  vault
)

// Kafka SASL
val jaasConfig = CredentialHelper.getKafkaSaslConfig(
  config.connectionParams,
  config.credentialId,
  vault
)
```

---

### 3. ✅ Extractor Trait Enhanced
**File**: `src/main/scala/com/etl/extract/Extractor.scala`

**Changes**:
```scala
trait Extractor {
  // Existing method (backward compatible)
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame

  // New method with vault access
  def extractWithVault(config: ExtractConfig, vault: CredentialVault)
                      (implicit spark: SparkSession): DataFrame = {
    extract(config)(spark)  // Default delegates to old method
  }
}
```

**Design**:
- Default implementation maintains backward compatibility
- Extractors override `extractWithVault()` for secure credentials
- Legacy `extract()` still works (with warnings)

---

### 4. ✅ PostgreSQLExtractor Updated
**File**: `src/main/scala/com/etl/extract/PostgreSQLExtractor.scala`

**Changes**:
- Overrides `extractWithVault()` instead of `extract()`
- Uses `CredentialHelper.getPasswordFromExtractConfig()`
- Legacy `extract()` creates temp InMemoryVault for backward compatibility

**Before**:
```scala
// ❌ Plain-text password
config.connectionParams.get("password").foreach { pwd =>
  reader = reader.option("password", pwd)
}
```

**After**:
```scala
// ✅ Secure vault retrieval
val password = CredentialHelper.getPasswordFromExtractConfig(config, vault)
reader = reader.option("password", password)
```

---

### 5. ✅ ETLPipeline Updated
**File**: `src/main/scala/com/etl/core/ETLPipeline.scala`

**Changes**:
```scala
// Before
val df = extractor.extract(context.config.extract)(context.spark)

// After
val df = extractor.extractWithVault(context.config.extract, context.vault)(context.spark)
```

---

## Remaining Work (40% - 3.2 hours)

### 6. ⏳ Update Remaining Extractors (1 hour)

#### MySQLExtractor
**File**: `src/main/scala/com/etl/extract/MySQLExtractor.scala`
```scala
// TODO: Same pattern as PostgreSQLExtractor
override def extractWithVault(config: ExtractConfig, vault: CredentialVault)
                              (implicit spark: SparkSession): DataFrame = {
  val password = CredentialHelper.getPasswordFromExtractConfig(config, vault)
  // ... use password in JDBC connection
}
```

#### KafkaExtractor
**File**: `src/main/scala/com/etl/extract/KafkaExtractor.scala`
```scala
// TODO: Add SASL support
override def extractWithVault(config: ExtractConfig, vault: CredentialVault)
                              (implicit spark: SparkSession): DataFrame = {
  val saslConfig = CredentialHelper.getKafkaSaslConfig(
    config.connectionParams, config.credentialId, vault
  )
  saslConfig.foreach { jaas =>
    configuredReader = configuredReader.option("kafka.sasl.jaas.config", jaas)
  }
}
```

#### S3Extractor
**File**: `src/main/scala/com/etl/extract/S3Extractor.scala`
```scala
// TODO: Isolate S3 credentials (not global)
override def extractWithVault(config: ExtractConfig, vault: CredentialVault)
                              (implicit spark: SparkSession): DataFrame = {
  val (accessKey, secretKey) = CredentialHelper.getS3Credentials(
    config.connectionParams, config.credentialId, vault
  )

  // TODO: Use per-operation credentials (not global spark.conf.set)
  // This requires Hadoop Configuration customization
}
```

---

### 7. ⏳ Update Loader Trait (15 minutes)

**File**: `src/main/scala/com/etl/load/Loader.scala`
```scala
trait Loader {
  def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult

  // Add vault-aware method
  def loadWithVault(df: DataFrame, config: LoadConfig, mode: WriteMode, vault: CredentialVault): LoadResult = {
    load(df, config, mode)  // Default delegates to old method
  }
}
```

---

### 8. ⏳ Update All Loaders (1.5 hours)

#### PostgreSQLLoader
```scala
override def loadWithVault(df: DataFrame, config: LoadConfig, mode: WriteMode, vault: CredentialVault): LoadResult = {
  val password = CredentialHelper.getPasswordFromLoadConfig(config, vault)
  // Use password in JDBC connection
}
```

**Apply same pattern to**:
- MySQLLoader
- KafkaLoader (SASL credentials)
- S3Loader (AWS credentials, isolated per-operation)

---

### 9. ⏳ Update ETLPipeline Load Stage (5 minutes)

**File**: `src/main/scala/com/etl/core/ETLPipeline.scala`
```scala
// Current
val result = loader.load(transformedDf, context.config.load, context.config.load.writeMode)

// Update to
val result = loader.loadWithVault(
  transformedDf,
  context.config.load,
  context.config.load.writeMode,
  context.vault
)
```

---

### 10. ⏳ Update PipelineExecutor (30 minutes)

**File**: `src/main/scala/com/etl/core/PipelineExecutor.scala`

**Changes Needed**:
```scala
class PipelineExecutor(config: PipelineConfig) {
  def execute()(implicit spark: SparkSession): Try[ExecutionMetrics] = {
    // Load vault
    val vaultPath = sys.env.getOrElse("VAULT_PATH", "/path/to/vault.enc")
    val vault = try {
      FileBasedVault(vaultPath)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to load vault from $vaultPath: ${e.getMessage}. Using empty vault.")
        InMemoryVault()
    }

    // Create context with vault
    val context = ExecutionContext.create(spark, config, vault)

    // Execute pipeline
    // ...
  }
}
```

---

### 11. ⏳ Update Example Configurations (30 minutes)

**Files**: `src/main/resources/configs/*.json`, `configs/*.json`

**Before** (❌ Plain-text):
```json
{
  "extract": {
    "sourceType": "postgresql",
    "connectionParams": {
      "host": "localhost",
      "database": "mydb",
      "user": "etl_user",
      "password": "secret123"  // ❌ Plain-text
    }
  }
}
```

**After** (✅ Secure):
```json
{
  "extract": {
    "sourceType": "postgresql",
    "credentialId": "postgres.production",  // ✅ Reference to vault
    "connectionParams": {
      "host": "localhost",
      "database": "mydb",
      "user": "etl_user"
      // No password in config
    }
  }
}
```

**Vault File** (`vault.enc`):
```json
{
  "postgres.production": "encrypted_base64_string",
  "s3.production.access": "encrypted_base64_string",
  "s3.production.secret": "encrypted_base64_string",
  "kafka.production.username": "encrypted_base64_string",
  "kafka.production.password": "encrypted_base64_string"
}
```

---

### 12. ⏳ Create Documentation (30 minutes)

**File**: `docs/CREDENTIAL_MANAGEMENT.md` (NEW)

**Topics**:
1. Why use CredentialVault
2. Setting up vault
3. Encrypting credentials
4. Configuring pipelines with credentialId
5. Vault management CLI examples
6. Rotating credentials
7. Production deployment with AWS Secrets Manager / HashiCorp Vault

---

## Configuration Examples

### Vault Setup

```bash
# Set master key (production: use AWS KMS, Secrets Manager, etc.)
export VAULT_MASTER_KEY="your-32-character-master-key-here"
export VAULT_PATH="/secure/path/to/vault.enc"
```

### Encrypt Credentials (Scala REPL)

```scala
import com.etl.config.FileBasedVault

val masterKey = sys.env("VAULT_MASTER_KEY")

// Encrypt passwords
val encryptedPostgresPassword = FileBasedVault.encrypt("secret123", masterKey)
val encryptedS3Access = FileBasedVault.encrypt("AKIAIOSFODNN7EXAMPLE", masterKey)
val encryptedS3Secret = FileBasedVault.encrypt("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", masterKey)

// Write to vault.enc
val vaultJson = s"""
{
  "postgres.production": "$encryptedPostgresPassword",
  "s3.production.access": "$encryptedS3Access",
  "s3.production.secret": "$encryptedS3Secret"
}
"""
```

### Pipeline Configuration

```json
{
  "pipelineId": "secure-etl-pipeline",
  "extract": {
    "sourceType": "postgresql",
    "credentialId": "postgres.production",
    "connectionParams": {
      "host": "prod-db.example.com",
      "database": "analytics",
      "user": "etl_user"
    }
  },
  "load": {
    "sinkType": "s3",
    "credentialId": "s3.production",
    "connectionParams": {
      "path": "s3a://my-bucket/output/",
      "format": "parquet"
    }
  }
}
```

---

## Testing

### Unit Tests to Add

```scala
class CredentialHelperSpec extends AnyFlatSpec with Matchers {
  "CredentialHelper" should "retrieve from vault first" in {
    val vault = InMemoryVault("test.password" -> "vault-pwd")
    val config = ExtractConfig(
      credentialId = Some("test.password"),
      connectionParams = Map("password" -> "config-pwd")
    )

    val result = CredentialHelper.getPasswordFromExtractConfig(config, vault)
    result shouldBe "vault-pwd"  // Vault takes precedence
  }

  it should "fall back to config if vault lookup fails" in {
    val vault = InMemoryVault()  // Empty
    val config = ExtractConfig(
      credentialId = Some("missing.password"),
      connectionParams = Map("password" -> "config-pwd")
    )

    val result = CredentialHelper.getPasswordFromExtractConfig(config, vault)
    result shouldBe "config-pwd"  // Falls back
  }

  it should "warn when using plain-text" in {
    // TODO: Capture logger warnings
  }
}
```

### Integration Tests

```scala
class CredentialVaultIntegrationSpec extends AnyFlatSpec with Matchers {
  "PostgreSQLExtractor" should "use vault for password" in {
    val vault = InMemoryVault("postgres.test" -> "test-pwd")
    val config = ExtractConfig(
      sourceType = "postgresql",
      credentialId = Some("postgres.test"),
      connectionParams = Map(
        "host" -> "localhost",
        "database" -> "testdb",
        "user" -> "test"
      )
    )

    val extractor = new PostgreSQLExtractor()
    // Mock database connection
    // Verify password retrieved from vault
  }
}
```

---

## Migration Path

### Phase 1: Soft Rollout (Current)
- ✅ Vault integration added
- ✅ Backward compatible with plain-text
- ⚠️ Warnings logged for plain-text usage

### Phase 2: Encourage Adoption (1 week)
- 📢 Update all documentation
- 📢 Update all example configs
- 📢 Team training on vault usage

### Phase 3: Enforce (2 weeks after Phase 2)
- ❌ Remove fallback to plain-text passwords
- ❌ Fail fast if credentialId not provided
- ❌ Security audit passes

---

## Security Best Practices

### ✅ DO
- Store vault file on secure, encrypted filesystem
- Use environment variable for `VAULT_MASTER_KEY`
- Rotate master key periodically
- Use AWS Secrets Manager / HashiCorp Vault in production
- Audit vault access logs
- Encrypt vault.enc file at rest

### ❌ DON'T
- Commit vault.enc to version control
- Hardcode VAULT_MASTER_KEY in code
- Share vault file via email/Slack
- Log decrypted credentials
- Use plain-text passwords in production

---

## Production Deployment

### AWS Deployment

```scala
// Use AWS Secrets Manager
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient

class AWSSecretsVault(secretName: String) extends CredentialVault {
  private val client = SecretsManagerClient.create()

  override def getCredential(id: String): String = {
    val response = client.getSecretValue(
      GetSecretValueRequest.builder()
        .secretId(s"$secretName/$id")
        .build()
    )
    response.secretString()
  }
}

// In PipelineExecutor
val vault = new AWSSecretsVault("etl-pipeline-secrets")
```

### Kubernetes Deployment

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: etl-vault-key
type: Opaque
data:
  master-key: <base64-encoded-master-key>
---
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: etl-pipeline
    env:
    - name: VAULT_MASTER_KEY
      valueFrom:
        secretKeyRef:
          name: etl-vault-key
          key: master-key
    - name: VAULT_PATH
      value: "/vault/vault.enc"
    volumeMounts:
    - name: vault-volume
      mountPath: /vault
  volumes:
  - name: vault-volume
    secret:
      secretName: etl-vault-file
```

---

## Rollback Plan

If issues arise:

1. **Revert ExecutionContext**: Remove `vault` parameter
2. **Revert extractors/loaders**: Continue using `extract()` / `load()`
3. **Keep CredentialHelper**: Can be used standalone
4. **No config changes needed**: Backward compatible

**Risk**: LOW - Backward compatibility maintained

---

## Summary

**Completed**:
- ✅ ExecutionContext enhanced with vault
- ✅ CredentialHelper utility created
- ✅ Extractor trait enhanced (backward compatible)
- ✅ PostgreSQLExtractor updated
- ✅ ETLPipeline extract stage updated

**Remaining** (3.2 hours):
- ⏳ Update 3 more extractors (MySQL, Kafka, S3)
- ⏳ Enhance Loader trait
- ⏳ Update 4 loaders (PostgreSQL, MySQL, Kafka, S3)
- ⏳ Update PipelineExecutor to initialize vault
- ⏳ Update example configurations
- ⏳ Create documentation

**Next Step**: Continue with MySQL Extractor, then remaining extractors and loaders following the same pattern as PostgreSQLExtractor.

---

**Status**: 60% Complete - Continue implementation following this guide.
