# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Project Overview

cluster-pulse is a standalone Scala 3 library for monitoring Apache Pekko cluster state with optional OpenTelemetry metrics export. It provides cluster status tracking, health scoring, split-brain detection, shard balance analysis, event diffing, and a Prometheus `/metrics` endpoint.

**Organization:** `dev.thenth`
**Current version:** `1.0.0`
**Scala version:** `3.8.1`

## Build & Test Commands

```bash
# Compile
sbt compile

# Run all tests
sbt test

# Run tests with coverage report
sbt coverage test coverageReport

# Check formatting
sbt scalafmtCheckAll

# Auto-format
sbt scalafmtAll
```

## Code Coverage

Coverage thresholds are enforced in `build.sbt`:
- **Statement coverage minimum:** 80%
- **Branch coverage minimum:** 75%
- Current actual coverage: ~88% statement / ~83% branch

Coverage reports are generated in `target/scala-3.8.1/scoverage-report/`.

## Architecture

The main package is `dev.thenth.clusterpulse` under `src/main/scala/`. Key components:

- **`ClusterPulse`** — Pekko Extension; singleton per `ActorSystem`, entry point for the library
- **`ClusterStatusTracker`** — Typed actor collecting cluster membership, shard distribution, and entity counts via Receptionist pattern
- **`ClusterStatusService` / `LiveClusterStatusService`** — Trait + implementation for one-shot and streaming cluster status access
- **`ClusterStatus`** — Core model with `ShardInfo`, `NodeInfo`, health score, shard balance score, and spray-json serialization
- **`OtelClusterReporter`** — Wires OpenTelemetry `ObservableLongGauge` metrics from cluster status snapshots
- **`ClusterHistory`** — Rolling history buffer (bounded circular buffer) for snapshot retention
- **`SplitBrainDetector`** — Three heuristics: unreachable majority, rapid membership change, no Up nodes
- **`ClusterEvent`** — Event diffing via `diff()` producing `NodeJoined`, `NodeLeft`, `NodeUnreachable`, `NodeReachable`, `ShardRebalanced`
- **`PrometheusMetricsRoute`** — Pekko HTTP route serving Prometheus exposition format at `GET /metrics`
- **`ClusterPulseSettings`** — Reads `cluster-pulse.*` config from `reference.conf` / `application.conf`

## Key Dependencies

- Apache Pekko (actor, cluster-sharding, stream, HTTP, serialization-jackson) — `1.6.0` / `1.3.0`
- spray-json — `1.3.6`
- OpenTelemetry API — `1.62.0` (compile scope; SDK is test-only)
- ScalaTest + Mockito — test scope

## Configuration

Library configuration lives in `src/main/resources/reference.conf` under the `cluster-pulse` namespace. Users override via `application.conf`.

## Formatting

Uses Scalafmt with Scala 3 dialect, 120 column width. Config in `.scalafmt.conf`.
