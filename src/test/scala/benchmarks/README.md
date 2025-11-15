# JMH Benchmarks for claude-spark-sbt

This directory contains JMH (Java Microbenchmark Harness) benchmarks for performance testing critical components of the ETL framework.

## Available Benchmarks

### 1. ETLPipelineBenchmark

Benchmarks for core ETL pipeline operations:

- **Extract operations**: Tests extraction with different record counts (1K, 10K, 100K)
- **Transform operations**: Tests aggregation performance
- **Load operations**: Tests load/write performance
- **Full pipeline**: End-to-end pipeline benchmarks
- **Caching impact**: Compares pipeline performance with and without caching
- **Partition tuning**: Tests different partition counts (2, 4, 8)

**Run all ETL benchmarks:**
```bash
sbt "Jmh/run -i 10 -wi 5 -f 1 benchmarks.ETLPipelineBenchmark"
```

**Run specific benchmark:**
```bash
sbt "Jmh/run -i 20 -wi 10 -f 2 ETLPipelineBenchmark.benchmarkFullPipeline"
```

### 2. RetryCircuitBreakerBenchmark

Benchmarks for resilience mechanisms:

- **Retry overhead**: Measures async retry performance
- **Retry strategies**: Compares FixedDelay vs ExponentialBackoff
- **Circuit breaker overhead**: Measures overhead in different states (Closed, Open, HalfOpen)
- **State transitions**: Tests circuit breaker state change performance
- **Concurrent usage**: Tests circuit breaker under concurrent load
- **Combined usage**: Tests retry + circuit breaker together
- **Delay calculations**: Benchmarks backoff calculation overhead
- **Failure rate tracking**: Tests circuit breaker failure rate calculation

**Run all retry/CB benchmarks:**
```bash
sbt "Jmh/run -i 10 -wi 5 -f 1 benchmarks.RetryCircuitBreakerBenchmark"
```

**Run specific benchmark:**
```bash
sbt "Jmh/run -i 15 -wi 5 RetryCircuitBreakerBenchmark.benchmarkAsyncRetry_NoFailure"
```

## Quick Start

### 1. Run All Benchmarks

```bash
sbt "Jmh/run -i 10 -wi 5 -f 1"
```

### 2. Run Benchmarks for Specific Class

```bash
sbt "Jmh/run -i 10 -wi 5 -f 1 ETLPipelineBenchmark"
```

### 3. Run Single Benchmark Method

```bash
sbt "Jmh/run -i 10 -wi 5 benchmarkFullPipeline"
```

### 4. List All Available Benchmarks

```bash
sbt "Jmh/run -l"
```

## JMH Command-Line Options

### Execution Parameters

- `-i <count>`: Number of measurement iterations (default: 10)
- `-wi <count>`: Number of warmup iterations (default: 5)
- `-f <count>`: Number of forks (default: 1)
- `-t <count>`: Number of threads (default: 1)

### Benchmark Modes

Use `-bm <mode>` to specify the benchmark mode:

- `thrpt`: Throughput (operations per second)
- `avgt`: Average time per operation (default for our benchmarks)
- `sample`: Samples the time for each operation
- `ss`: Single shot (measures cold startup)
- `all`: Run all modes

**Example:**
```bash
sbt "Jmh/run -i 10 -bm thrpt ETLPipelineBenchmark.benchmarkFullPipeline"
```

### Output Options

- `-o <file>`: Write results to file
- `-rf <format>`: Result format (text, csv, json, latex, scsv)
- `-rff <file>`: Result file format

**Example - Generate CSV report:**
```bash
sbt "Jmh/run -i 10 -rf csv -rff results.csv ETLPipelineBenchmark"
```

### Profiling

- `-prof gc`: GC profiler (garbage collection statistics)
- `-prof stack`: Stack profiler (hottest methods)
- `-prof perf`: Linux perf profiler (CPU performance counters)

**Example with GC profiling:**
```bash
sbt "Jmh/run -i 10 -prof gc ETLPipelineBenchmark.benchmarkFullPipeline"
```

## Example Workflows

### 1. Quick Performance Check

Run a quick benchmark to get rough performance numbers:

```bash
sbt "Jmh/run -i 3 -wi 1 -f 1 benchmarkFullPipeline"
```

### 2. Detailed Performance Analysis

Run a thorough benchmark with multiple forks for statistical significance:

```bash
sbt "Jmh/run -i 20 -wi 10 -f 3 -rf json -rff detailed-results.json ETLPipelineBenchmark"
```

### 3. Compare Configurations

Run benchmarks with different partition counts and compare:

```bash
sbt "Jmh/run -i 15 -wi 5 benchmarkPipeline_2Partitions benchmarkPipeline_4Partitions benchmarkPipeline_8Partitions"
```

### 4. Throughput Testing

Measure how many operations can be performed per second:

```bash
sbt "Jmh/run -i 10 -wi 5 -bm thrpt ETLPipelineBenchmark"
```

### 5. Latency Distribution

Get percentile distribution of operation times:

```bash
sbt "Jmh/run -i 10 -wi 5 -bm sample ETLPipelineBenchmark.benchmarkFullPipeline"
```

### 6. Memory Profiling

Analyze memory allocation and GC behavior:

```bash
sbt "Jmh/run -i 10 -wi 5 -prof gc -prof stack ETLPipelineBenchmark.benchmarkFullPipeline"
```

## Interpreting Results

### Example Output

```
Benchmark                                        Mode  Cnt    Score    Error  Units
ETLPipelineBenchmark.benchmarkFullPipeline       avgt   10  123.456 ± 5.678  ms/op
```

- **Mode**: Benchmark mode (avgt = average time)
- **Cnt**: Number of measurements
- **Score**: Average time/throughput
- **Error**: Margin of error (95% confidence interval)
- **Units**: Measurement units (ms/op = milliseconds per operation)

### What to Look For

1. **Score**: Lower is better for time-based metrics, higher is better for throughput
2. **Error margin**: Smaller error means more consistent performance
3. **Units**: Make sure you understand what's being measured
4. **Iteration variance**: Check if scores vary significantly between iterations

### Performance Baselines

Based on local development environment (these will vary by hardware):

**ETL Pipeline Benchmarks:**
- Extract 1K records: ~5-10 ms
- Extract 10K records: ~20-50 ms
- Extract 100K records: ~200-500 ms
- Full pipeline (10K records): ~100-200 ms
- Pipeline with caching: ~80-150 ms (20-30% faster)

**Retry/Circuit Breaker Benchmarks:**
- No retry overhead: ~1-2 μs
- Async retry (no failure): ~10-50 μs
- Async retry (1 failure): ~100-500 μs
- Circuit breaker closed: ~5-10 μs overhead
- Circuit breaker open (fail-fast): ~1-5 μs

## Best Practices

### 1. Adequate Warmup

Always include sufficient warmup iterations (5-10) to allow JVM JIT compilation:

```bash
sbt "Jmh/run -i 10 -wi 10"
```

### 2. Multiple Forks

Use multiple forks (2-3) for statistical significance:

```bash
sbt "Jmh/run -i 10 -f 3"
```

### 3. Isolated Environment

- Close unnecessary applications
- Ensure consistent CPU/memory availability
- Disable CPU frequency scaling if possible
- Run on dedicated hardware for production benchmarks

### 4. Reproducibility

- Use fixed random seeds in benchmark code
- Document hardware specifications
- Record JVM version and flags
- Commit benchmark configurations to version control

### 5. Comparison Testing

When comparing changes:

```bash
# Baseline
git checkout main
sbt "Jmh/run -i 20 -f 3 -rf json -rff baseline.json"

# Your changes
git checkout feature-branch
sbt "Jmh/run -i 20 -f 3 -rf json -rff feature.json"

# Compare results
```

## Continuous Integration

### Running in CI

For CI environments, use reduced iterations to save time:

```bash
sbt "Jmh/run -i 5 -wi 2 -f 1"
```

### Performance Regression Detection

Set up CI to fail if performance degrades beyond threshold:

```bash
# Run benchmarks and check for regressions
sbt "Jmh/run -i 10 -wi 5 -f 1 -rf json -rff ci-results.json"

# Compare with baseline (requires custom script)
./scripts/check-performance-regression.sh ci-results.json baseline.json
```

## Troubleshooting

### Issue: Benchmarks take too long

**Solution**: Reduce iterations or use faster benchmarks
```bash
sbt "Jmh/run -i 3 -wi 1 -f 1"
```

### Issue: High variance in results

**Causes**:
- Insufficient warmup
- Background processes competing for resources
- Thermal throttling
- GC pauses

**Solutions**:
- Increase warmup iterations: `-wi 10`
- Use multiple forks: `-f 3`
- Profile GC: `-prof gc`
- Close background applications

### Issue: Out of memory errors

**Solution**: Increase JVM heap size in build.sbt:
```scala
Jmh / javaOptions += "-Xmx4G"
```

### Issue: Spark UI port conflicts

Spark UI is disabled in benchmarks via:
```scala
.config("spark.ui.enabled", "false")
```

If you see port conflicts, ensure no other Spark jobs are running.

## Adding New Benchmarks

### 1. Create Benchmark Class

```scala
package benchmarks

import org.openjdk.jmh.annotations._
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
class MyNewBenchmark {

  @Benchmark
  def benchmarkMyFeature(): Unit = {
    // Your benchmark code
  }
}
```

### 2. Place in src/test/scala/benchmarks/

All benchmark classes should be in this directory.

### 3. Run Your Benchmark

```bash
sbt "Jmh/run -i 10 MyNewBenchmark"
```

## Resources

- [JMH Documentation](https://github.com/openjdk/jmh)
- [JMH Samples](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/)
- [Avoiding Benchmarking Pitfalls](https://www.oracle.com/technical-resources/articles/java/architect-benchmarking.html)
- [sbt-jmh Plugin](https://github.com/sbt/sbt-jmh)

## Makefile Integration

You can also use the Makefile for convenience:

```bash
# Run all benchmarks
make benchmark

# Run quick benchmarks
make benchmark-quick

# Run with profiling
make benchmark-profile
```

(Add these targets to the Makefile if they don't exist)
