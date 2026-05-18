# cluster-pulse

[![CI](https://github.com/TheNthDev/cluster-pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/TheNthDev/cluster-pulse/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/dev.thenth/cluster-pulse_3)](https://central.sonatype.com/artifact/dev.thenth/cluster-pulse_3)
[![codecov](https://codecov.io/gh/TheNthDev/cluster-pulse/branch/main/graph/badge.svg?token=R1EKBZ5YY2)](https://codecov.io/gh/TheNthDev/cluster-pulse)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A standalone, reusable Scala library for monitoring [Apache Pekko](https://pekko.apache.org/) cluster state with optional [OpenTelemetry](https://opentelemetry.io/) metrics export.

## Features

- **`ClusterStatusTracker`** — A Pekko Typed actor that collects cluster membership, shard distribution, and entity counts across all nodes via the Receptionist pattern. Supports **multiple entity type keys**.
- **`ClusterStatusService` / `LiveClusterStatusService`** — A trait + implementation providing both one-shot (`Future`) and streaming (`Source`) access to cluster status.
- **`OtelClusterReporter`** — Wires `ObservableLongGauge` metrics into the OpenTelemetry SDK, updated on a configurable timer. Metrics are point-in-time snapshots, never accumulated.
- **`ClusterPulse`** — A true Pekko `Extension` accessible via `ClusterPulse(system)`. Singleton per `ActorSystem`; auto-creates tracker, service, history buffer, and split-brain detector from config. `StandaloneClusterPulse` available via `ClusterPulse.create(...)` for custom configurations.
- **`ClusterPulseSettings`** — Reads `cluster-pulse.*` from `reference.conf` / `application.conf` for configurable intervals and timeouts.
- **Health Score** — `ClusterStatus.healthScore` (0–100) and `isHealthy` for Kubernetes readiness probes.
- **Shard Balance Score** — `ClusterStatus.shardBalanceScore` (0.0–1.0) detects hot nodes via coefficient of variation.
- **Event Log / Changelog** — `ClusterEvent.diff()` compares consecutive snapshots and emits structured events: `NodeJoined`, `NodeLeft`, `NodeUnreachable`, `NodeReachable`, `ShardRebalanced`. Full JSON serialization support.
- **Rolling History Buffer** — `ClusterHistory` retains the last N snapshots in a bounded, thread-safe circular buffer. Provides `nodeCountDelta`, event log, and snapshot access.
- **Split-Brain Detection** — `SplitBrainDetector` uses three heuristics (unreachable majority, rapid membership change, no Up nodes) to flag potential network partitions. Exposes `isDetected` and `reason`.
- **Distributed Tracing** — Optional OTel `Tracer` integration adds spans to the `gatherClusterStats` fan-out cycle, with `cluster.node.count` and `cluster.entity.count` attributes.
- **Prometheus `/metrics` Endpoint** — `PrometheusMetricsRoute` serves all metrics in Prometheus exposition format via Pekko HTTP, including split-brain and history gauges.
- **`ClusterPulseSerializable`** — Marker trait for cluster serialization; ships with a `reference.conf` binding to `jackson-cbor`.

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "dev.thenth.clusterpulse" %% "cluster-pulse" % "1.0.0"
```

## OpenTelemetry SDK — Bring Your Own

cluster-pulse depends on `opentelemetry-api` at **compile** scope but does **not** ship the OpenTelemetry SDK at runtime. If you use `OtelClusterReporter` or distributed tracing, you must add the SDK (or a compatible implementation) to your own dependencies:

```scala
libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk" % "1.62.0"
```

This keeps the library lightweight for projects that only need cluster status without metrics export.

## Quick Start

### 1. Using the `ClusterPulse` Pekko Extension (recommended)

The simplest way to use cluster-pulse. `ClusterPulse(system)` returns a singleton extension that auto-creates the tracker, service, history buffer, and split-brain detector from your `application.conf`.

```scala
import dev.thenth.clusterpulse._
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}

// Access the extension — singleton per ActorSystem
val pulse = ClusterPulse(system)

// Auto-discover entity types via shardingInit
val myEntityRef = pulse.shardingInit(Entity(MyEntity.TypeKey)(createBehavior))

// Or register keys manually
pulse.registerEntity(AnotherEntity.TypeKey)
```

### 2. Standalone instance with custom configuration

Use `ClusterPulse.create(...)` when you need full control over optional components (OTel reporter, custom history size, tracer, etc.).

```scala
import dev.thenth.clusterpulse._
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}

val sharding = ClusterSharding(system)

// Simple: auto-discovery, no extras
val pulse = ClusterPulse.create(system, sharding)

// Full-featured: multiple type keys, OTel, history, split-brain, tracing
val history  = new ClusterHistory(120)
val sbd      = new SplitBrainDetector()
val reporter = new OtelClusterReporter(otel, Some(sbd), Some(history))
val tracer   = otel.getTracer("my-service")

val pulse = ClusterPulse.create(
  system, sharding,
  Seq(EntityA.TypeKey, EntityB.TypeKey),
  Some(reporter), Some(history), Some(sbd), Some(tracer)
)
```

### 3. Querying Status and Health

```scala
// Query
val status: Future[ClusterStatus] = pulse.status
val stream: Source[ClusterStatus, ?] = pulse.statusStream()

// Health checks (e.g., for K8s readiness probes)
val healthy: Boolean = status.map(_.isHealthy)
val score: Int       = status.map(_.healthScore)    // 0–100
val balance: Double  = status.map(_.shardBalanceScore) // 0.0–1.0
```

### 4. Manual actor spawning

```scala
import dev.thenth.clusterpulse._

val history = new ClusterHistory(60)
val sbd     = new SplitBrainDetector()

val tracker = context.spawn(
  ClusterStatusTracker(sharding, Seq(EntityA.TypeKey), None, Some(history), Some(sbd), None),
  "Tracker"
)

val service = new LiveClusterStatusService(tracker)
```

### 5. Event log / changelog

```scala
import dev.thenth.clusterpulse.model.ClusterEvent

// Via ClusterHistory (automatic diffing on each record)
val events: List[ClusterEvent] = history.events

// Manual diffing
val changes = ClusterEvent.diff(previousStatus, currentStatus, System.currentTimeMillis())
changes.foreach {
  case ClusterEvent.NodeJoined(ts, addr, roles) => println(s"$addr joined")
  case ClusterEvent.ShardRebalanced(ts, id, from, to) => println(s"Shard $id moved $from -> $to")
  case _ => // ...
}
```

### 6. Split-brain detection

```scala
val sbd = new SplitBrainDetector(membershipChangeThreshold = 0.5)
sbd.update(clusterStatus)

if (sbd.isDetected) {
  println(s"Split-brain suspected: ${sbd.reason.get}")
}
```

### 7. Prometheus endpoint

```scala
import dev.thenth.clusterpulse.PrometheusMetricsRoute

val metricsRoute = PrometheusMetricsRoute(
  () => reporter.currentStatus,
  Some(splitBrainDetector),
  Some(history)
)

// Add to your Pekko HTTP routes:
val allRoutes = yourRoutes ~ metricsRoute.route
// GET /metrics returns Prometheus exposition format
```

### 8. OpenTelemetry configuration

Set standard `OTEL_*` environment variables:

```bash
OTEL_SERVICE_NAME=my-pekko-service
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_METRICS_EXPORTER=otlp
OTEL_METRIC_EXPORT_INTERVAL=15000
```

## Configuration

All settings are in `reference.conf` and can be overridden in your `application.conf`:

```hocon
cluster-pulse {
  # Interval between OTel ReportMetrics ticks
  report-interval = 15s

  # Timeout for ask operations (GetStatus, GetLocalState, Receptionist lookups)
  ask-timeout = 5s

  # Interval between status snapshots in the SSE / streaming source
  stream-interval = 2s

  # Number of snapshots to retain in the rolling history buffer
  history-buffer-size = 60

  # Fraction of nodes that must change between snapshots to trigger
  # the split-brain rapid-membership-change heuristic (0.0–1.0)
  split-brain-membership-threshold = 0.5

  # Whether to include individual entity IDs in status snapshots.
  # Set to false to reduce serialization and memory overhead on large clusters.
  include-entity-ids = true
}
```

## Metrics

### OTel Gauges

| Metric | Description |
|---|---|
| `cluster.pulse.node.count` | Total cluster members |
| `cluster.pulse.node.unreachable` | Unreachable members |
| `cluster.pulse.entity.count` | Total active entities |
| `cluster.pulse.shard.count` | Total active shards across all nodes (cluster-wide) |
| `cluster.pulse.shard.region.count` | Active shards per shard region (`shard_region` label) |
| `cluster.pulse.health.score` | Composite health score (0–100) |
| `cluster.pulse.shard.balance` | Shard balance: 0.0 = balanced, 1.0 = imbalanced |
| `cluster.pulse.node.entity.count` | Entity count per node (`node_address` label) |
| `cluster.pulse.node.status` | 1 = Up, 0 = Unreachable (`node_address` label) |
| `cluster.pulse.split_brain.detected` | 1 = suspected, 0 = healthy (when detector provided) |
| `cluster.pulse.node.count.delta` | Node count change over history window (when history provided) |
| `cluster.pulse.history.size` | Snapshots in history buffer (when history provided) |

### Prometheus Endpoint

All the above metrics are also available at `GET /metrics` in Prometheus exposition format when `PrometheusMetricsRoute` is wired into your HTTP server.

## Observability Stack (Grafana + Prometheus)

The `observability/` directory contains two ready-to-use Docker Compose setups for visualizing cluster-pulse metrics with Grafana and Prometheus. Both include a pre-provisioned Grafana instance with a Prometheus datasource and a cluster-pulse dashboard.

### Option A: Prometheus Endpoint Scrape (default)

Prometheus scrapes your application's `/metrics` endpoint directly via `PrometheusMetricsRoute`.

1. Start your application with the `PrometheusMetricsRoute` serving on port `8085` (or update `observability/prometheus/prometheus.yml` to match your port).

2. Launch the observability stack from the project root:

   ```bash
   docker compose -f observability/docker-compose.yml up -d
   ```

3. Open Grafana at [http://localhost:3000](http://localhost:3000) (default credentials: `admin` / `admin`).

4. The **Cluster Pulse** dashboard is automatically provisioned and available under **Dashboards**.

### Option B: OTel Collector Push (no public metrics endpoint)

Your application pushes metrics to an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) via OTLP gRPC. The Collector exposes a Prometheus-compatible endpoint that Prometheus scrapes internally — your application never serves a public `/metrics` endpoint.

```
Your App  ──OTLP/gRPC──▶  OTel Collector  ◀──scrape──  Prometheus  ◀──query──  Grafana
  (push, no /metrics)      (internal:8889)              (internal:9090)
```

1. Add the OTel SDK and OTLP exporter to your `build.sbt`:

   ```scala
   libraryDependencies ++= Seq(
     "io.opentelemetry" % "opentelemetry-sdk"           % "1.62.0",
     "io.opentelemetry" % "opentelemetry-exporter-otlp" % "1.62.0"
   )
   ```

2. Configure the OTel SDK to export via OTLP gRPC to the Collector:

   ```scala
   import io.opentelemetry.sdk.OpenTelemetrySdk
   import io.opentelemetry.sdk.metrics.SdkMeterProvider
   import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
   import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter

   val otlpExporter = OtlpGrpcMetricExporter.builder()
     .setEndpoint("http://localhost:4317") // Collector gRPC endpoint
     .build()

   val meterProvider = SdkMeterProvider.builder()
     .registerMetricReader(
       PeriodicMetricReader.builder(otlpExporter)
         .setInterval(java.time.Duration.ofSeconds(15))
         .build()
     )
     .build()

   val otel = OpenTelemetrySdk.builder()
     .setMeterProvider(meterProvider)
     .build()

   val reporter = new OtelClusterReporter(otel, Some(sbd), Some(history))
   ```

3. Launch the OTel Collector observability stack from the project root:

   ```bash
   docker compose -f observability/docker-compose-otel.yml up -d
   ```

4. Open Grafana at [http://localhost:3000](http://localhost:3000) (default credentials: `admin` / `admin`).

### Customization

- **Scrape target (Option A)** — Edit `observability/prometheus/prometheus.yml` to change the target host/port.
- **Collector config (Option B)** — Edit `observability/otel-collector/config.yml` to add processors, exporters, or additional receivers.
- **Dashboard** — Modify or replace `observability/grafana/dashboards/cluster-pulse.json` to customize panels.
- **Datasources** — Edit `observability/grafana/provisioning/datasources/prometheus.yml` to add additional datasources.

## Alternatives Comparison

No existing open-source package provides Pekko cluster + shard + entity metrics with native OTel export. Here's how cluster-pulse compares:

| Feature | cluster-pulse | Pekko Mgmt HTTP | Cinnamon | Kamon | OTel Agent | JMX Exporter | Micrometer |
|---|---|---|---|---|---|---|---|
| **Pekko support** | ✅ | ✅ | ❌ Akka only | ❌ Akka only | ✅ (JVM only) | ⚠️ Fragile | ✅ |
| **Cluster membership** | ✅ | ✅ | ✅ | ✅ | ❌ | ⚠️ | ✅ (manual) |
| **Shard-level metrics** | ✅ | ❌ | ✅ | ⚠️ Basic | ❌ | ⚠️ | ✅ (manual) |
| **Per-node entity count** | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ (manual) |
| **Entity enumeration** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ (manual) |
| **Unreachable detection** | ✅ | ✅ | ✅ | ✅ | ❌ | ⚠️ | ✅ (manual) |
| **Native OTel export** | ✅ | ❌ | ✅ | ⚠️ | ✅ | ⚠️ Indirect | ✅ Via bridge |
| **Health score** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Shard balance score** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Split-brain detection** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Event changelog** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Rolling history** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Distributed tracing** | ✅ | ❌ | ✅ | ⚠️ | ✅ | ❌ | ❌ |
| **Prometheus endpoint** | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ |
| **Multi-type-key support** | ✅ | N/A | ✅ | ⚠️ | ❌ | ❌ | ✅ (manual) |
| **Zero-code setup** | ❌ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ❌ |
| **Open source** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| **SSE/streaming API** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Cost** | Free | Free | Paid | Free | Free | Free | Free |

### Key Takeaways

1. **cluster-pulse fills a gap** — no existing open-source tool provides Pekko cluster + shard + entity OTel metrics.
2. **OTel Java Agent is complementary** — use it for JVM/HTTP metrics alongside cluster-pulse for cluster topology.
3. **The trade-off is maintenance** — cluster-pulse must track Pekko API changes, whereas agent-based solutions auto-adapt. For Pekko projects this is unavoidable since no auto-instrumentation exists for cluster internals.

## Code Coverage

Run tests with [scoverage](https://github.com/scoverage/sbt-scoverage) instrumentation and generate an HTML report:

```bash
sbt clean coverage test coverageReport
```

The HTML report will be available at `target/scala-3.8.1/scoverage-report/index.html`.

## License

Apache 2.0
