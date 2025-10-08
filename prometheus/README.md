# ETL Pipeline Monitoring Setup

Complete guide for setting up Prometheus and Grafana monitoring for the ETL framework.

## Quick Start

### 1. Start Metrics Server in Your Pipeline

```scala
import com.etl.monitoring.{MetricsHttpServer, ETLMetrics}
import com.etl.core.ETLPipeline

object Main {
  def main(args: Array[String]): Unit = {
    // Start metrics server on port 9090
    val metricsServer = MetricsHttpServer.fromEnv() // Reads METRICS_PORT env var

    try {
      // Run your pipeline - metrics are automatically collected
      val pipeline = ETLPipeline(extractor, transformers, loader)
      val result = pipeline.run(context)

      println(s"Pipeline completed. Metrics available at http://localhost:9090/metrics")

      // Keep server running for scraping
      Thread.sleep(300000) // 5 minutes

    } finally {
      metricsServer.stop()
    }
  }
}
```

### 2. Start Prometheus

```bash
# Download Prometheus (if not installed)
wget https://github.com/prometheus/prometheus/releases/download/v2.40.0/prometheus-2.40.0.linux-amd64.tar.gz
tar xvfz prometheus-2.40.0.linux-amd64.tar.gz
cd prometheus-2.40.0.linux-amd64

# Copy configuration
cp /path/to/prometheus.yml .
cp /path/to/alerting-rules.yml .

# Start Prometheus
./prometheus --config.file=prometheus.yml
```

Prometheus UI: http://localhost:9090

### 3. Start Grafana

```bash
# Download Grafana (if not installed)
wget https://dl.grafana.com/oss/release/grafana-9.3.2.linux-amd64.tar.gz
tar -zxvf grafana-9.3.2.linux-amd64.tar.gz
cd grafana-9.3.2

# Start Grafana
./bin/grafana-server
```

Grafana UI: http://localhost:3000 (default login: admin/admin)

### 4. Import Dashboard

1. Open Grafana UI
2. Go to Dashboards → Import
3. Upload `grafana/etl-pipeline-dashboard.json`
4. Select Prometheus datasource
5. Click Import

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  ETL Pipeline Application                                   │
│                                                             │
│  ┌──────────────────┐      ┌──────────────────┐           │
│  │  ETLPipeline     │─────→│  ETLMetrics      │           │
│  │  (business logic)│      │  (metric updates)│           │
│  └──────────────────┘      └──────────────────┘           │
│                                     │                       │
│                                     ▼                       │
│                          ┌──────────────────┐              │
│                          │ MetricsRegistry  │              │
│                          │ (central store)  │              │
│                          └──────────────────┘              │
│                                     │                       │
│                                     ▼                       │
│                          ┌──────────────────┐              │
│                          │MetricsHttpServer │              │
│                          │  :9090/metrics   │              │
│                          └──────────────────┘              │
└─────────────────────────────────────┬───────────────────────┘
                                      │ HTTP scrape every 15s
                                      ▼
                           ┌──────────────────┐
                           │   Prometheus     │
                           │   :9090          │
                           │                  │
                           │ - Stores metrics │
                           │ - Evaluates rules│
                           │ - Sends alerts   │
                           └──────────────────┘
                                      │
                       ┌──────────────┴──────────────┐
                       │                             │
                       ▼                             ▼
            ┌──────────────────┐        ┌──────────────────┐
            │    Grafana       │        │  Alertmanager    │
            │    :3000         │        │    :9093         │
            │                  │        │                  │
            │ - Dashboards     │        │ - Alert routing  │
            │ - Visualization  │        │ - Deduplication  │
            └──────────────────┘        │ - Notifications  │
                                        └──────────────────┘
```

## Available Metrics

### Pipeline Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_pipeline_executions_total` | Counter | `pipeline_id`, `status` | Total pipeline executions |
| `etl_pipeline_duration_seconds` | Histogram | `pipeline_id`, `status` | Pipeline execution duration |
| `etl_active_pipelines` | Gauge | `mode` | Currently running pipelines |

### Record Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_records_extracted_total` | Counter | `pipeline_id`, `source_type` | Total records extracted |
| `etl_records_transformed_total` | Counter | `pipeline_id`, `transform_type` | Total records transformed |
| `etl_records_loaded_total` | Counter | `pipeline_id`, `sink_type`, `write_mode` | Total records loaded |
| `etl_records_failed_total` | Counter | `pipeline_id`, `stage`, `reason` | Total failed records |
| `etl_record_batch_size` | Histogram | `pipeline_id`, `stage` | Distribution of batch sizes |

### Performance Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_extract_duration_seconds` | Histogram | `pipeline_id`, `source_type` | Extract stage duration |
| `etl_transform_duration_seconds` | Histogram | `pipeline_id`, `transform_type` | Transform stage duration |
| `etl_load_duration_seconds` | Histogram | `pipeline_id`, `sink_type` | Load stage duration |

### Data Quality Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_data_quality_checks_total` | Counter | `pipeline_id`, `rule_type`, `severity`, `status` | Total quality checks |
| `etl_data_quality_violations_total` | Counter | `pipeline_id`, `rule_name`, `severity` | Total quality violations |
| `etl_schema_validation_failures_total` | Counter | `pipeline_id`, `stage`, `schema_name` | Schema validation failures |

### Streaming Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_streaming_batches_processed_total` | Counter | `pipeline_id`, `query_name`, `status` | Total streaming batches |
| `etl_streaming_batch_duration_seconds` | Histogram | `pipeline_id`, `query_name` | Batch processing duration |
| `etl_streaming_lag_seconds` | Gauge | `pipeline_id`, `query_name` | Streaming lag |
| `etl_streaming_active_queries` | Gauge | - | Active streaming queries |
| `etl_streaming_duplicate_batches_total` | Counter | `pipeline_id`, `query_name` | Duplicate batches (exactly-once) |
| `etl_batch_tracker_records` | Gauge | `pipeline_id` | Batch tracker table size |

### Error Handling Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_retries_total` | Counter | `pipeline_id`, `stage`, `strategy` | Total retry attempts |
| `etl_circuit_breaker_state` | Gauge | `pipeline_id`, `component` | Circuit breaker state (0=closed, 1=half_open, 2=open) |
| `etl_circuit_breaker_transitions_total` | Counter | `pipeline_id`, `component`, `from_state`, `to_state` | State transitions |
| `etl_dlq_records_total` | Counter | `pipeline_id`, `stage`, `dlq_type` | Records sent to DLQ |

### Resource Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etl_spark_executors` | Gauge | - | Active Spark executors |
| `etl_spark_active_jobs` | Gauge | - | Active Spark jobs |
| `etl_spark_active_stages` | Gauge | - | Active Spark stages |

## Example Queries

### Pipeline Success Rate

```promql
100 * rate(etl_pipeline_executions_total{status="success"}[5m])
  / (rate(etl_pipeline_executions_total[5m]) + 0.0001)
```

### P95 Pipeline Duration

```promql
histogram_quantile(0.95,
  sum(rate(etl_pipeline_duration_seconds_bucket[5m])) by (le, pipeline_id)
)
```

### Record Throughput

```promql
rate(etl_records_loaded_total[5m])
```

### Top Pipelines by Failures

```promql
topk(5,
  sum(rate(etl_records_failed_total[1h])) by (pipeline_id)
)
```

### Data Quality Violation Rate

```promql
100 * rate(etl_data_quality_violations_total[5m])
  / (rate(etl_data_quality_checks_total[5m]) + 0.0001)
```

### Streaming Lag

```promql
etl_streaming_lag_seconds > 60
```

## Alerting

Alerts are defined in `alerting-rules.yml`. Key alerts include:

### Pipeline Health
- High failure rate (> 5%)
- Critical failure rate (> 20%)
- Pipeline stopped executing

### Performance
- Slow execution (p95 > 5 minutes)
- Throughput drop (> 50% decrease)

### Data Quality
- High violations (> 1% error-level)
- Critical violations (> 10% error-level)
- Schema validation failures

### Streaming
- High lag (> 5 minutes)
- Critical lag (> 15 minutes)
- No active queries
- Batch processing failures

### Error Handling
- Circuit breaker open
- High retry rate (> 10/s)
- DLQ backlog (> 50 records/s)

### Resources
- Low Spark executors (< 2)
- No Spark executors

### Alert Configuration

Edit `prometheus.yml` to configure Alertmanager:

```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - alertmanager:9093
```

Configure Alertmanager routes, receivers, and notification channels in `alertmanager.yml`.

## Production Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./alerting-rules.yml:/etc/prometheus/alerting-rules.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml
      - alertmanager-data:/alertmanager

volumes:
  prometheus-data:
  grafana-data:
  alertmanager-data:
```

### Kubernetes Deployment

Use Prometheus Operator for Kubernetes:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: etl-pipeline-metrics
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

### Environment Variables

Configure the metrics server port:

```bash
export METRICS_PORT=9090
```

Or in your Spark submit:

```bash
spark-submit \
  --conf spark.driver.extraJavaOptions="-DMETRICS_PORT=9090" \
  --class com.etl.Main \
  etl-pipeline.jar
```

## Dashboard Panels

The Grafana dashboard includes:

1. **Active Pipelines** - Gauge showing current running pipelines
2. **Pipeline Execution Rate** - Time series of success/failure rates
3. **Success Rate %** - Gauge showing overall success percentage
4. **Pipeline Duration Percentiles** - p50, p95, p99 latencies
5. **Record Throughput by Stage** - Extract, transform, load rates
6. **Failed Records by Reason** - Stacked area chart of failures
7. **Data Quality Violations** - Violations by rule and severity
8. **Streaming Batch Processing Rate** - Batches processed per second
9. **Streaming Lag** - Lag in seconds with thresholds

## Troubleshooting

### Metrics not showing in Prometheus

1. Check metrics server is running:
   ```bash
   curl http://localhost:9090/metrics
   ```

2. Check Prometheus targets:
   - Open http://localhost:9090/targets
   - Verify etl-pipeline target is UP

3. Check Prometheus logs:
   ```bash
   tail -f /var/log/prometheus.log
   ```

### Dashboard shows "No data"

1. Verify Prometheus datasource in Grafana:
   - Configuration → Data Sources → Prometheus
   - Test connection

2. Check time range:
   - Adjust time picker in top-right corner

3. Verify metrics exist in Prometheus:
   - Open Prometheus UI → Graph
   - Query: `etl_pipeline_executions_total`

### Alerts not firing

1. Check alerting rules loaded:
   - Prometheus UI → Alerts

2. Verify Alertmanager connection:
   - Prometheus UI → Status → Runtime & Build Information

3. Check Alertmanager logs:
   ```bash
   tail -f /var/log/alertmanager.log
   ```

## Best Practices

1. **Metric Naming**: Follow Prometheus conventions
   - Use `_total` suffix for counters
   - Use base units (seconds, bytes)
   - Use snake_case

2. **Cardinality**: Limit label values
   - Avoid high-cardinality labels (user IDs, timestamps)
   - Use descriptive but bounded label values

3. **Retention**: Configure based on needs
   - Short-term: 15 days in Prometheus
   - Long-term: Use remote storage (Thanos, Cortex)

4. **Scrape Interval**: Balance freshness vs load
   - 15s for production pipelines
   - 1m for development/testing

5. **Alerting**: Follow SRE principles
   - Alert on symptoms, not causes
   - Reduce false positives
   - Provide clear runbook URLs

## References

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
- [Alertmanager Documentation](https://prometheus.io/docs/alerting/latest/alertmanager/)
