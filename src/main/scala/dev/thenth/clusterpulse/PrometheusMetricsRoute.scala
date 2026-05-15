package dev.thenth.clusterpulse

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpCharsets, HttpEntity, MediaType, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import dev.thenth.clusterpulse.model.ClusterStatus

/**
 * Optional Pekko HTTP route that exposes cluster-pulse metrics in Prometheus
 * exposition format at `/metrics`. Useful for environments that run Prometheus
 * but not an OpenTelemetry Collector.
 *
 * Usage:
 * {{{
 * val metricsRoute = PrometheusMetricsRoute(statusProvider, splitBrainDetector, history)
 * // Concatenate with your existing routes:
 * val allRoutes = yourRoutes ~ metricsRoute.route
 * }}}
 */
class PrometheusMetricsRoute(
  statusProvider: () => ClusterStatus,
  splitBrainDetector: Option[SplitBrainDetector] = None,
  history: Option[ClusterHistory] = None
) {

  private val prometheusTextType: ContentType.WithFixedCharset = ContentType.WithFixedCharset(
    MediaType.customWithFixedCharset(
      "text", "plain",
      HttpCharsets.`UTF-8`,
      params = Map("version" -> "0.0.4")
    )
  )

  val route: Route = path("metrics") {
    get {
      val status = statusProvider()
      val sb     = new StringBuilder

      // Node count
      sb.append("# HELP cluster_pulse_node_count Total cluster members\n")
      sb.append("# TYPE cluster_pulse_node_count gauge\n")
      sb.append(s"cluster_pulse_node_count ${status.nodes.size}\n")

      // Unreachable count
      sb.append("# HELP cluster_pulse_node_unreachable Unreachable cluster members\n")
      sb.append("# TYPE cluster_pulse_node_unreachable gauge\n")
      sb.append(s"cluster_pulse_node_unreachable ${status.unreachableCount}\n")

      // Entity count
      sb.append("# HELP cluster_pulse_entity_count Total active entities\n")
      sb.append("# TYPE cluster_pulse_entity_count gauge\n")
      sb.append(s"cluster_pulse_entity_count ${status.totalEntityCount}\n")

      // Shard count
      sb.append("# HELP cluster_pulse_shard_count Total active shards\n")
      sb.append("# TYPE cluster_pulse_shard_count gauge\n")
      sb.append(s"cluster_pulse_shard_count ${status.nodes.flatMap(_.shards).size}\n")

      // Per-region shard count
      val allShards = status.nodes.flatMap(_.shards)
      val byRegion = allShards.filter(_.regionType.nonEmpty).groupBy(_.regionType)
      if (byRegion.nonEmpty) {
        sb.append("# HELP cluster_pulse_shard_region_count Active shards per shard region\n")
        sb.append("# TYPE cluster_pulse_shard_region_count gauge\n")
        sb.append("# HELP cluster_pulse_entity_region_count Entity count per shard region\n")
        sb.append("# TYPE cluster_pulse_entity_region_count gauge\n")
        byRegion.foreach { case (region, shards) =>
          sb.append(s"""cluster_pulse_shard_region_count{shard_region="$region"} ${shards.size}\n""")
          sb.append(s"""cluster_pulse_entity_region_count{shard_region="$region"} ${shards.map(_.entityCount).sum}\n""")
        }
      }

      // Health score
      sb.append("# HELP cluster_pulse_health_score Composite health score 0-100\n")
      sb.append("# TYPE cluster_pulse_health_score gauge\n")
      sb.append(s"cluster_pulse_health_score ${status.healthScore}\n")

      // Shard balance
      sb.append("# HELP cluster_pulse_shard_balance Shard balance score 0.0-1.0\n")
      sb.append("# TYPE cluster_pulse_shard_balance gauge\n")
      sb.append(s"cluster_pulse_shard_balance ${status.shardBalanceScore}\n")

      // Per-node entity count
      sb.append("# HELP cluster_pulse_node_entity_count Entity count per node\n")
      sb.append("# TYPE cluster_pulse_node_entity_count gauge\n")
      status.nodes.foreach { node =>
        val count = node.shards.map(_.entityCount).sum
        sb.append(s"""cluster_pulse_node_entity_count{node_address="${node.address}"} $count\n""")
      }

      // Per-node status
      sb.append("# HELP cluster_pulse_node_status Node reachability 1=Up 0=Unreachable\n")
      sb.append("# TYPE cluster_pulse_node_status gauge\n")
      status.nodes.foreach { node =>
        val v = if (node.status == "Unreachable") 0 else 1
        sb.append(s"""cluster_pulse_node_status{node_address="${node.address}"} $v\n""")
      }

      // Split-brain detection
      splitBrainDetector.foreach { sbd =>
        sb.append("# HELP cluster_pulse_split_brain_detected Split-brain suspected 1=yes 0=no\n")
        sb.append("# TYPE cluster_pulse_split_brain_detected gauge\n")
        sb.append(s"cluster_pulse_split_brain_detected ${if (sbd.isDetected) 1 else 0}\n")
      }

      // History node count delta
      history.foreach { h =>
        sb.append("# HELP cluster_pulse_node_count_delta Node count change over history window\n")
        sb.append("# TYPE cluster_pulse_node_count_delta gauge\n")
        sb.append(s"cluster_pulse_node_count_delta ${h.nodeCountDelta}\n")
      }

      complete(HttpEntity(prometheusTextType, sb.toString()))
    }
  }
}

object PrometheusMetricsRoute {

  def apply(
    statusProvider: () => ClusterStatus,
    splitBrainDetector: Option[SplitBrainDetector] = None,
    history: Option[ClusterHistory] = None
  ): PrometheusMetricsRoute = new PrometheusMetricsRoute(statusProvider, splitBrainDetector, history)
}
