# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-05-15

### Added
- `ClusterPulse` Pekko Extension — singleton per `ActorSystem` via `ClusterPulse(system)`.
- `StandaloneClusterPulse` via `ClusterPulse.create(...)` for custom configurations.
- Auto-discovery of entity type keys via `ClusterPulse.shardingInit`.
- `PrometheusMetricsRoute` — serves all metrics at `GET /metrics` in Prometheus exposition format.
- Split-brain detection with three heuristics: unreachable majority, rapid membership change, no Up nodes.
- Rolling history buffer (`ClusterHistory`) with configurable snapshot retention.
- Event changelog via `ClusterEvent.diff()` — emits `NodeJoined`, `NodeLeft`, `NodeUnreachable`, `NodeReachable`, `ShardRebalanced`.
- Distributed tracing spans via optional OTel `Tracer`.
- `include-entity-ids` config flag to disable entity ID enumeration for large clusters.
- Multi-entity type key support in `ClusterStatusTracker`.
- `reference.conf`-driven configuration via `ClusterPulseSettings`.
- Health score (`healthScore` 0–100) and `isHealthy` readiness signal.
- Shard balance score using coefficient of variation.
- `OtelClusterReporter` with 11 `ObservableLongGauge` callbacks.
- `ClusterStatusService` trait and `LiveClusterStatusService` implementation.
- `ClusterStatusTracker` actor with Receptionist-based fan-out.
- `ClusterPulseSerializable` marker trait with `reference.conf` jackson-cbor binding.
- `ClusterStatus` model with `ShardInfo`, `NodeInfo`, and spray-json serialization.
- Standalone `build.sbt` with Maven Central publish support.
- README with usage examples, configuration, and alternatives comparison.

### Fixed
- Non-hosting nodes no longer penalize the health score (`shardBalanceScore` filters empty nodes).
- Stale `/metrics` after stream failure resolved with `RestartSource.withBackoff`.

[Unreleased]: https://github.com/TheNthDev/cluster-pulse/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/TheNthDev/cluster-pulse/releases/tag/v1.0.0
