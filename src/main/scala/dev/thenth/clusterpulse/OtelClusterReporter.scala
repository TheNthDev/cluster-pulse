package dev.thenth.clusterpulse

import java.util.concurrent.atomic.AtomicReference
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import dev.thenth.clusterpulse.model.ClusterStatus

/** Bridges cluster-pulse metrics into OpenTelemetry by registering `ObservableLongGauge` callbacks on the OTel `Meter`.
  *
  * Call [[update]] on every reporting tick to refresh the latest [[ClusterStatus]] snapshot; the OTel SDK reads the
  * gauges on its own export interval.
  *
  * @param otel
  *   the OpenTelemetry instance to register metrics with
  * @param splitBrainDetector
  *   optional detector whose state is exported as a gauge
  * @param history
  *   optional history buffer whose size and delta are exported
  */
class OtelClusterReporter(
  otel: OpenTelemetry,
  splitBrainDetector: Option[SplitBrainDetector] = None,
  history: Option[ClusterHistory] = None
) {

  private val meter  = otel.getMeter("cluster-pulse")
  private val latest = new AtomicReference[ClusterStatus](ClusterStatus(Nil, 0, Nil))

  // Called by the actor on every ReportMetrics tick
  def update(status: ClusterStatus): Unit = latest.set(status)

  def currentStatus: ClusterStatus = latest.get()

  // OTel SDK invokes these callbacks on its own export interval
  meter
    .gaugeBuilder("cluster.pulse.node.count")
    .ofLongs()
    .setDescription("Total cluster members")
    .buildWithCallback(m => m.record(latest.get().nodes.size))

  meter
    .gaugeBuilder("cluster.pulse.entity.count")
    .ofLongs()
    .setDescription("Total active entities")
    .buildWithCallback(m => m.record(latest.get().totalEntityCount))

  meter
    .gaugeBuilder("cluster.pulse.node.unreachable")
    .ofLongs()
    .setDescription("Unreachable cluster members")
    .buildWithCallback(m => m.record(latest.get().unreachableCount))

  meter
    .gaugeBuilder("cluster.pulse.shard.count")
    .ofLongs()
    .setDescription("Total active shards across all nodes (cluster-wide)")
    .buildWithCallback(m => m.record(latest.get().nodes.flatMap(_.shards).size))

  meter
    .gaugeBuilder("cluster.pulse.shard.region.count")
    .ofLongs()
    .setDescription("Active shards per shard region (use shard_region label to filter)")
    .buildWithCallback { m =>
      val allShards = latest.get().nodes.flatMap(_.shards)
      allShards.groupBy(_.regionType).foreach { case (region, shards) =>
        if (region.nonEmpty) {
          val attrs = Attributes.of(AttributeKey.stringKey("shard_region"), region)
          m.record(shards.size.toLong, attrs)
        }
      }
    }

  meter
    .gaugeBuilder("cluster.pulse.health.score")
    .ofLongs()
    .setDescription("Composite cluster health score (0-100)")
    .buildWithCallback(m => m.record(latest.get().healthScore))

  meter
    .gaugeBuilder("cluster.pulse.shard.balance")
    .setDescription("Shard balance score: 0.0 = balanced, 1.0 = imbalanced")
    .buildWithCallback(m => m.record(latest.get().shardBalanceScore))

  meter
    .gaugeBuilder("cluster.pulse.node.entity.count")
    .ofLongs()
    .setDescription("Entity count per node")
    .buildWithCallback { m =>
      latest.get().nodes.foreach { node =>
        val attrs = Attributes.of(AttributeKey.stringKey("node_address"), node.address)
        m.record(node.shards.map(_.entityCount).sum, attrs)
      }
    }

  meter
    .gaugeBuilder("cluster.pulse.node.status")
    .ofLongs()
    .setDescription("Node reachability: 1 = Up, 0 = Unreachable")
    .buildWithCallback { m =>
      latest.get().nodes.foreach { node =>
        val attrs = Attributes.of(AttributeKey.stringKey("node_address"), node.address)
        m.record(if node.status == "Unreachable" then 0L else 1L, attrs)
      }
    }

  // Split-brain detection gauge
  splitBrainDetector.foreach { sbd =>
    meter
      .gaugeBuilder("cluster.pulse.split_brain.detected")
      .ofLongs()
      .setDescription("Split-brain suspected: 1 = yes, 0 = no")
      .buildWithCallback(m => m.record(if sbd.isDetected then 1L else 0L))
  }

  // History node count delta gauge
  history.foreach { h =>
    meter
      .gaugeBuilder("cluster.pulse.node.count.delta")
      .ofLongs()
      .setDescription("Node count change over the history window")
      .buildWithCallback(m => m.record(h.nodeCountDelta.toLong))

    meter
      .gaugeBuilder("cluster.pulse.history.size")
      .ofLongs()
      .setDescription("Number of snapshots in the history buffer")
      .buildWithCallback(m => m.record(h.size.toLong))
  }
}
