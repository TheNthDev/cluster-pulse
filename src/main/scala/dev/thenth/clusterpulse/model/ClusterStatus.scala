package dev.thenth.clusterpulse.model

import spray.json.DefaultJsonProtocol.*
import spray.json.RootJsonFormat

case class ShardInfo(shardId: String, entityCount: Int, entityIds: List[String], regionType: String = "")

case class NodeInfo(address: String, status: String, roles: Set[String], shards: List[ShardInfo])

case class ClusterStatus(
  nodes: List[NodeInfo],
  totalEntityCount: Int,
  activeEntities: List[String]
) {

  /** Number of unreachable members. */
  def unreachableCount: Int = nodes.count(_.status == "Unreachable")

  /** True when the cluster has at least one node and no unreachable members. */
  def isHealthy: Boolean = nodes.nonEmpty && unreachableCount == 0

  /** Composite health score (0–100).
    *   - 0 if the cluster is empty.
    *   - Deducts points for unreachable nodes (proportional to cluster size).
    *   - Deducts points for shard imbalance.
    */
  def healthScore: Int = {
    if (nodes.isEmpty) return 0
    val reachableRatio = 1.0 - unreachableCount.toDouble / nodes.size
    val reachableScore = reachableRatio * 70.0            // 70% weight on reachability
    val balanceScore   = (1.0 - shardBalanceScore) * 30.0 // 30% weight on balance
    math.max(0, math.min(100, (reachableScore + balanceScore).toInt))
  }

  /** Shard balance score: 0.0 = perfectly balanced, 1.0 = maximally imbalanced. Computed as the coefficient of
    * variation (stddev / mean) of per-node entity counts, clamped to [0.0, 1.0]. Returns 0.0 when there are fewer than
    * 2 shard-hosting nodes or no entities. Nodes with no shards (non-hosting nodes) are excluded from the calculation
    * so they don't penalize the balance score.
    */
  def shardBalanceScore: Double = {
    val hostingNodes = nodes.filter(_.shards.nonEmpty)
    val counts       = hostingNodes.map(_.shards.map(_.entityCount).sum.toDouble)
    if (counts.size < 2) return 0.0
    val mean = counts.sum / counts.size
    if (mean == 0.0) return 0.0
    val variance = counts.map(c => (c - mean) * (c - mean)).sum / counts.size
    val stddev   = math.sqrt(variance)
    math.min(1.0, stddev / mean)
  }
}

object ClusterStatus {
  implicit val shardInfoFormat: RootJsonFormat[ShardInfo]         = jsonFormat4(ShardInfo.apply)
  implicit val nodeInfoFormat: RootJsonFormat[NodeInfo]           = jsonFormat4(NodeInfo.apply)
  implicit val clusterStatusFormat: RootJsonFormat[ClusterStatus] = jsonFormat3(ClusterStatus.apply)
}
