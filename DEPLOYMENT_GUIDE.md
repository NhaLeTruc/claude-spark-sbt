# ETL Pipeline Framework - Production Deployment Guide

Complete guide for deploying the Spark-based ETL framework to production.

**Version**: 1.0.0
**Last Updated**: 2025-10-08

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Build and Package](#build-and-package)
3. [Deployment Architectures](#deployment-architectures)
4. [Spark Submit Deployment](#spark-submit-deployment)
5. [Kubernetes Deployment](#kubernetes-deployment)
6. [AWS EMR Deployment](#aws-emr-deployment)
7. [Configuration Management](#configuration-management)
8. [Monitoring Setup](#monitoring-setup)
9. [Security](#security)
10. [Operational Procedures](#operational-procedures)

---

## Prerequisites

### Software Requirements

- **Java**: OpenJDK 11 or later
- **Scala**: 2.12.18
- **SBT**: 1.9.x
- **Apache Spark**: 3.5.6
- **PostgreSQL/MySQL**: For metadata and batch tracking
- **Kafka**: For streaming pipelines (optional)
- **Prometheus + Grafana**: For monitoring (recommended)

### Infrastructure Requirements

**Minimum (Development)**:
- 4 CPU cores
- 16 GB RAM
- 100 GB storage

**Production (Small)**:
- 8 CPU cores
- 32 GB RAM
- 500 GB storage
- Network bandwidth: 1 Gbps

**Production (Large)**:
- 32+ CPU cores
- 128+ GB RAM
- 2+ TB storage
- Network bandwidth: 10 Gbps

---

## Build and Package

### 1. Build JAR

```bash
cd /path/to/claude-spark-sbt

# Clean and compile
sbt clean compile

# Run tests
sbt test

# Package JAR
sbt assembly

# Output: target/scala-2.12/etl-pipeline-assembly-1.0.0.jar
```

### 2. Verify Build

```bash
# Check JAR contents
jar tf target/scala-2.12/etl-pipeline-assembly-1.0.0.jar | head -20

# Check main class
jar xf target/scala-2.12/etl-pipeline-assembly-1.0.0.jar META-INF/MANIFEST.MF
cat META-INF/MANIFEST.MF
```

### 3. Build Docker Image (Optional)

```dockerfile
# Dockerfile
FROM openjdk:11-jre-slim

# Install Spark
ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3.3.6
RUN apt-get update && apt-get install -y wget procps && \
    wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy JAR and configs
COPY target/scala-2.12/etl-pipeline-assembly-1.0.0.jar /app/
COPY configs/ /app/configs/

WORKDIR /app

# Expose metrics port
EXPOSE 9090

# Entry point
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
```

Build:

```bash
docker build -t etl-pipeline:1.0.0 .
docker tag etl-pipeline:1.0.0 your-registry/etl-pipeline:1.0.0
docker push your-registry/etl-pipeline:1.0.0
```

---

## Deployment Architectures

### Architecture 1: Standalone Spark Cluster

```
┌─────────────────────────────────────────────────────────┐
│                  Load Balancer / HAProxy                │
└────────────────────┬────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│Spark Master │ │Spark Master │ │Spark Master │
│   (Standby) │ │  (Active)   │ │  (Standby)  │
└─────────────┘ └─────────────┘ └─────────────┘
        │            │            │
        └────────────┼────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│   Worker    │ │   Worker    │ │   Worker    │
│  Executor   │ │  Executor   │ │  Executor   │
└─────────────┘ └─────────────┘ └─────────────┘
```

**Use Case**: On-premises, full control
**Pros**: Simple, low latency
**Cons**: Manual scaling, maintenance overhead

### Architecture 2: Kubernetes (Spark Operator)

```
┌─────────────────────────────────────────────────────────┐
│              Kubernetes Cluster (EKS/GKE)               │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │             Spark Operator                        │ │
│  └───────────────────────────────────────────────────┘ │
│                       │                                 │
│       ┌───────────────┼───────────────┐                │
│       │               │               │                │
│       ▼               ▼               ▼                │
│  ┌────────┐      ┌────────┐      ┌────────┐          │
│  │ Driver │      │ Driver │      │ Driver │          │
│  │  Pod   │      │  Pod   │      │  Pod   │          │
│  └────────┘      └────────┘      └────────┘          │
│       │               │               │                │
│  ┌────┴────┐     ┌────┴────┐     ┌────┴────┐         │
│  │Executor │     │Executor │     │Executor │         │
│  │  Pods   │     │  Pods   │     │  Pods   │         │
│  └─────────┘     └─────────┘     └─────────┘         │
└─────────────────────────────────────────────────────────┘
```

**Use Case**: Cloud-native, auto-scaling
**Pros**: Auto-scaling, resource efficiency, cloud integration
**Cons**: More complex setup

### Architecture 3: AWS EMR

```
┌─────────────────────────────────────────────────────────┐
│                    AWS EMR Cluster                      │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │              Master Node (m5.xlarge)              │ │
│  └───────────────────────────────────────────────────┘ │
│                       │                                 │
│       ┌───────────────┼───────────────┐                │
│       │               │               │                │
│       ▼               ▼               ▼                │
│  ┌────────┐      ┌────────┐      ┌────────┐          │
│  │  Core  │      │  Core  │      │  Core  │          │
│  │  Node  │      │  Node  │      │  Node  │          │
│  │(r5.2xl)│      │(r5.2xl)│      │(r5.2xl)│          │
│  └────────┘      └────────┘      └────────┘          │
│                                                         │
│  ┌────────┐      ┌────────┐      ┌────────┐          │
│  │  Task  │      │  Task  │      │  Task  │          │
│  │  Node  │      │  Node  │      │  Node  │          │
│  │ (Spot) │      │ (Spot) │      │ (Spot) │          │
│  └────────┘      └────────┘      └────────┘          │
└─────────────────────────────────────────────────────────┘
```

**Use Case**: AWS cloud, managed Spark
**Pros**: Fully managed, auto-scaling, spot instances
**Cons**: AWS-specific, higher cost

---

## Spark Submit Deployment

### Basic Deployment

```bash
spark-submit \
  --class com.etl.Main \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --conf spark.app.name="ETL Pipeline" \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.executor.instances=5 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --files configs/pipeline-config.json \
  --jars lib/postgresql-42.6.0.jar,lib/mysql-connector-java-8.0.33.jar \
  etl-pipeline-assembly-1.0.0.jar \
  --config configs/pipeline-config.json
```

### Production Configuration

```bash
#!/bin/bash
# deploy-production.sh

set -e

# Configuration
APP_NAME="ETL Pipeline - Production"
MASTER_URL="spark://spark-master-prod:7077"
JAR_PATH="/opt/etl/etl-pipeline-assembly-1.0.0.jar"
CONFIG_PATH="/opt/etl/configs/production.json"

# Spark Configuration
DRIVER_MEMORY="8g"
EXECUTOR_MEMORY="16g"
EXECUTOR_CORES="8"
NUM_EXECUTORS="10"
SHUFFLE_PARTITIONS="400"

# JVM Options
DRIVER_JAVA_OPTS="-Dlog4j.configuration=file:///opt/etl/log4j.properties"
DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -DMETRICS_PORT=9090"
DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -Dcom.sun.management.jmxremote"
DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -Dcom.sun.management.jmxremote.port=9999"
DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
DRIVER_JAVA_OPTS="$DRIVER_JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"

EXECUTOR_JAVA_OPTS="-Dlog4j.configuration=file:///opt/etl/log4j.properties"

# External Dependencies
POSTGRESQL_JAR="/opt/etl/lib/postgresql-42.6.0.jar"
MYSQL_JAR="/opt/etl/lib/mysql-connector-java-8.0.33.jar"
KAFKA_JAR="/opt/etl/lib/spark-sql-kafka-0-10_2.12-3.5.6.jar"

# Submit job
spark-submit \
  --class com.etl.Main \
  --master "$MASTER_URL" \
  --deploy-mode cluster \
  --name "$APP_NAME" \
  --conf "spark.driver.memory=$DRIVER_MEMORY" \
  --conf "spark.executor.memory=$EXECUTOR_MEMORY" \
  --conf "spark.executor.cores=$EXECUTOR_CORES" \
  --conf "spark.executor.instances=$NUM_EXECUTORS" \
  --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS" \
  --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTS" \
  --conf "spark.executor.extraJavaOptions=$EXECUTOR_JAVA_OPTS" \
  --conf "spark.dynamicAllocation.enabled=true" \
  --conf "spark.dynamicAllocation.minExecutors=5" \
  --conf "spark.dynamicAllocation.maxExecutors=20" \
  --conf "spark.dynamicAllocation.initialExecutors=10" \
  --conf "spark.shuffle.service.enabled=true" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryoserializer.buffer.max=512m" \
  --conf "spark.network.timeout=600s" \
  --conf "spark.executor.heartbeatInterval=60s" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.connection.maximum=200" \
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \
  --conf "spark.hadoop.fs.s3a.multipart.size=104857600" \
  --files "$CONFIG_PATH" \
  --jars "$POSTGRESQL_JAR,$MYSQL_JAR,$KAFKA_JAR" \
  "$JAR_PATH" \
  --config "$(basename $CONFIG_PATH)"

echo "Pipeline submitted successfully"
```

### Monitoring Spark Submit

```bash
# Monitor driver logs
spark-submit \
  ... \
  2>&1 | tee /var/log/etl/pipeline-$(date +%Y%m%d-%H%M%S).log

# Check application status
curl http://spark-master:8080/api/v1/applications

# View Spark UI
open http://spark-master:8080
```

---

## Kubernetes Deployment

### 1. Install Spark Operator

```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --set metrics.enable=true
```

### 2. Create SparkApplication CRD

```yaml
# etl-pipeline-spark-app.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: etl-pipeline-production
  namespace: data-pipelines
spec:
  type: Scala
  mode: cluster
  image: your-registry/etl-pipeline:1.0.0
  imagePullPolicy: Always
  mainClass: com.etl.Main
  mainApplicationFile: local:///app/etl-pipeline-assembly-1.0.0.jar
  arguments:
    - "--config"
    - "/app/configs/production.json"

  sparkVersion: "3.5.6"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20

  driver:
    cores: 2
    coreLimit: "2000m"
    memory: "8g"
    labels:
      version: "1.0.0"
      app: "etl-pipeline"
      component: "driver"
    serviceAccount: spark-driver-sa
    env:
      - name: METRICS_PORT
        value: "9090"
    ports:
      - name: metrics
        containerPort: 9090
        protocol: TCP

  executor:
    cores: 4
    instances: 5
    memory: "16g"
    labels:
      version: "1.0.0"
      app: "etl-pipeline"
      component: "executor"
    env:
      - name: SPARK_WORKER_MEMORY
        value: "16g"

  deps:
    jars:
      - local:///app/lib/postgresql-42.6.0.jar
      - local:///app/lib/mysql-connector-java-8.0.33.jar
    files:
      - local:///app/configs/production.json

  sparkConf:
    "spark.sql.shuffle.partitions": "400"
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.minExecutors": "2"
    "spark.dynamicAllocation.maxExecutors": "20"
    "spark.dynamicAllocation.initialExecutors": "5"
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    "spark.sql.adaptive.enabled": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.kubernetes.allocation.batch.size": "10"
    "spark.kubernetes.allocation.batch.delay": "1s"

  monitoring:
    exposeDriverMetrics: true
    exposeExecutorMetrics: true
    prometheus:
      jmxExporterJar: "/prometheus/jmx_prometheus_javaagent-0.17.0.jar"
      port: 8090
```

### 3. Deploy to Kubernetes

```bash
# Create namespace
kubectl create namespace data-pipelines

# Create service account
kubectl create serviceaccount spark-driver-sa -n data-pipelines

# Grant permissions
kubectl create clusterrolebinding spark-driver-role \
  --clusterrole=edit \
  --serviceaccount=data-pipelines:spark-driver-sa

# Deploy SparkApplication
kubectl apply -f etl-pipeline-spark-app.yaml

# Check status
kubectl get sparkapplications -n data-pipelines
kubectl describe sparkapplication etl-pipeline-production -n data-pipelines

# View logs
kubectl logs -f etl-pipeline-production-driver -n data-pipelines

# Port-forward metrics
kubectl port-forward -n data-pipelines svc/etl-pipeline-production-driver-svc 9090:9090
```

### 4. Create Service for Metrics

```yaml
# etl-pipeline-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: etl-pipeline-metrics
  namespace: data-pipelines
  labels:
    app: etl-pipeline
spec:
  type: ClusterIP
  ports:
    - name: metrics
      port: 9090
      targetPort: 9090
      protocol: TCP
  selector:
    app: etl-pipeline
    component: driver
```

### 5. ServiceMonitor for Prometheus

```yaml
# etl-pipeline-servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: etl-pipeline-metrics
  namespace: data-pipelines
  labels:
    app: etl-pipeline
spec:
  selector:
    matchLabels:
      app: etl-pipeline
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

---

## AWS EMR Deployment

### 1. Create EMR Cluster

```bash
aws emr create-cluster \
  --name "ETL Pipeline Cluster" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --ec2-attributes KeyName=your-key-pair,SubnetId=subnet-xxxxx \
  --instance-type m5.xlarge \
  --instance-count 5 \
  --use-default-roles \
  --log-uri s3://your-bucket/emr-logs/ \
  --bootstrap-actions \
    Path=s3://your-bucket/bootstrap/install-dependencies.sh
```

### 2. Bootstrap Script

```bash
#!/bin/bash
# install-dependencies.sh

set -e

# Install additional packages
sudo yum install -y postgresql-jdbc mysql-connector-java

# Copy JARs to Spark classpath
sudo cp /usr/share/java/postgresql-jdbc.jar /usr/lib/spark/jars/
sudo cp /usr/share/java/mysql-connector-java.jar /usr/lib/spark/jars/

# Download ETL pipeline JAR
aws s3 cp s3://your-bucket/jars/etl-pipeline-assembly-1.0.0.jar /home/hadoop/

# Download configs
aws s3 cp s3://your-bucket/configs/ /home/hadoop/configs/ --recursive

echo "Bootstrap completed successfully"
```

### 3. Submit Job to EMR

```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="ETL Pipeline",ActionOnFailure=CONTINUE,Args=[\
--class,com.etl.Main,\
--deploy-mode,cluster,\
--conf,spark.driver.memory=8g,\
--conf,spark.executor.memory=16g,\
--conf,spark.executor.cores=4,\
--conf,spark.dynamicAllocation.enabled=true,\
--conf,spark.dynamicAllocation.minExecutors=5,\
--conf,spark.dynamicAllocation.maxExecutors=20,\
s3://your-bucket/jars/etl-pipeline-assembly-1.0.0.jar,\
--config,s3://your-bucket/configs/production.json\
]
```

### 4. EMR Step Function (Automated)

```json
{
  "Comment": "ETL Pipeline on EMR",
  "StartAt": "CreateEMRCluster",
  "States": {
    "CreateEMRCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "ETL Pipeline Cluster",
        "ReleaseLabel": "emr-6.15.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": {
          "InstanceGroups": [
            {
              "Name": "Master",
              "InstanceRole": "MASTER",
              "InstanceType": "m5.xlarge",
              "InstanceCount": 1
            },
            {
              "Name": "Core",
              "InstanceRole": "CORE",
              "InstanceType": "r5.2xlarge",
              "InstanceCount": 3
            }
          ],
          "KeepJobFlowAliveWhenNoSteps": true,
          "TerminationProtected": false
        },
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole"
      },
      "ResultPath": "$.cluster",
      "Next": "RunSparkJob"
    },
    "RunSparkJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId",
        "Step": {
          "Name": "ETL Pipeline",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
              "spark-submit",
              "--class", "com.etl.Main",
              "--deploy-mode", "cluster",
              "s3://your-bucket/jars/etl-pipeline-assembly-1.0.0.jar",
              "--config", "s3://your-bucket/configs/production.json"
            ]
          }
        }
      },
      "ResultPath": "$.step",
      "Next": "TerminateCluster"
    },
    "TerminateCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId"
      },
      "End": true
    }
  }
}
```

---

## Configuration Management

### Environment-Specific Configs

```
configs/
├── local.json          # Local development
├── development.json    # Dev environment
├── staging.json        # Staging environment
└── production.json     # Production environment
```

### Using Environment Variables

```bash
# Set environment
export ETL_ENV=production
export CONFIG_PATH="/opt/etl/configs/${ETL_ENV}.json"
export METRICS_PORT=9090

# Credential vault encryption key
export VAULT_ENCRYPTION_KEY="your-32-character-encryption-key"

# AWS credentials (if not using IAM roles)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Database credentials
export POSTGRES_PASSWORD="your-postgres-password"
export MYSQL_PASSWORD="your-mysql-password"
```

### AWS Secrets Manager Integration

```scala
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

object SecretsManagerVault {
  def getSecret(secretName: String): String = {
    val client = SecretsManagerClient.create()
    val request = GetSecretValueRequest.builder()
      .secretId(secretName)
      .build()

    val response = client.getSecretValue(request)
    response.secretString()
  }
}
```

---

## Monitoring Setup

### 1. Deploy Prometheus

```bash
# Using Helm
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/prometheus \
  --namespace monitoring \
  --create-namespace \
  --set server.persistentVolume.size=100Gi
```

### 2. Deploy Grafana

```bash
helm install grafana grafana/grafana \
  --namespace monitoring \
  --set persistence.enabled=true \
  --set persistence.size=10Gi \
  --set adminPassword=admin
```

### 3. Import Dashboard

```bash
# Get Grafana admin password
kubectl get secret --namespace monitoring grafana -o jsonpath="{.data.admin-password}" | base64 --decode

# Port-forward Grafana
kubectl port-forward -n monitoring svc/grafana 3000:80

# Import dashboard: grafana/etl-pipeline-dashboard.json
```

---

## Security

### 1. Credential Management

**Never** store credentials in:
- Source code
- Configuration files (plaintext)
- Environment variables (for production)

**Use**:
- AWS Secrets Manager
- HashiCorp Vault
- Kubernetes Secrets
- Encrypted credential vault (framework feature)

### 2. Network Security

```yaml
# NetworkPolicy example
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: etl-pipeline-network-policy
  namespace: data-pipelines
spec:
  podSelector:
    matchLabels:
      app: etl-pipeline
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - protocol: TCP
          port: 9090
  egress:
    - to:
        - namespaceSelector: {}
      ports:
        - protocol: TCP
          port: 5432  # PostgreSQL
        - protocol: TCP
          port: 3306  # MySQL
        - protocol: TCP
          port: 9092  # Kafka
        - protocol: TCP
          port: 443   # HTTPS (S3, etc.)
```

### 3. IAM Roles (AWS)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-data-bucket/*",
        "arn:aws:s3:::your-data-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:etl/*"
    }
  ]
}
```

---

## Operational Procedures

### Daily Operations

1. **Check Pipeline Status**
   ```bash
   kubectl get sparkapplications -n data-pipelines
   ```

2. **View Metrics**
   - Open Grafana dashboard
   - Check Prometheus alerts

3. **Review Logs**
   ```bash
   kubectl logs -f etl-pipeline-production-driver -n data-pipelines
   ```

### Weekly Maintenance

1. **Clean up old checkpoints**
   ```bash
   find /checkpoints -type d -mtime +30 -exec rm -rf {} +
   ```

2. **Review batch tracker table**
   ```sql
   SELECT pipeline_id, COUNT(*) as batch_count
   FROM batch_tracker
   GROUP BY pipeline_id;
   ```

3. **Update dashboard screenshots for documentation**

### Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed troubleshooting guide.

---

## Best Practices

1. **Always test in staging first**
2. **Use blue-green deployments for zero-downtime**
3. **Monitor metrics from day one**
4. **Set up alerts before production**
5. **Keep configs in version control**
6. **Document all configuration changes**
7. **Use IAM roles instead of access keys**
8. **Enable audit logging**
9. **Regular security reviews**
10. **Disaster recovery plan**

---

**Version**: 1.0.0
**Last Updated**: 2025-10-08
