package dev.thenth.clusterpulse.model

import spray.json.DefaultJsonProtocol.*
import spray.json.{JsString, JsValue, JsonFormat, RootJsonFormat}

/** A structured event emitted when cluster membership changes between snapshots. */
sealed trait ClusterEvent {
  def timestamp: Long
  def eventType: String
}

object ClusterEvent {

  case class NodeJoined(timestamp: Long, address: String, roles: Set[String]) extends ClusterEvent {
    def eventType = "NodeJoined"
  }

  case class NodeLeft(timestamp: Long, address: String, roles: Set[String]) extends ClusterEvent {
    def eventType = "NodeLeft"
  }

  case class NodeUnreachable(timestamp: Long, address: String) extends ClusterEvent {
    def eventType = "NodeUnreachable"
  }

  case class NodeReachable(timestamp: Long, address: String) extends ClusterEvent {
    def eventType = "NodeReachable"
  }

  case class ShardRebalanced(timestamp: Long, shardId: String, fromNode: String, toNode: String) extends ClusterEvent {
    def eventType = "ShardRebalanced"
  }

  // JSON formats
  implicit val nodeJoinedFormat: RootJsonFormat[NodeJoined]           = jsonFormat3(NodeJoined.apply)
  implicit val nodeLeftFormat: RootJsonFormat[NodeLeft]               = jsonFormat3(NodeLeft.apply)
  implicit val nodeUnreachableFormat: RootJsonFormat[NodeUnreachable] = jsonFormat2(NodeUnreachable.apply)
  implicit val nodeReachableFormat: RootJsonFormat[NodeReachable]     = jsonFormat2(NodeReachable.apply)
  implicit val shardRebalancedFormat: RootJsonFormat[ShardRebalanced] = jsonFormat4(ShardRebalanced.apply)

  implicit val clusterEventFormat: RootJsonFormat[ClusterEvent] = new RootJsonFormat[ClusterEvent] {
    import spray.json.*

    override def write(obj: ClusterEvent): JsValue = {
      val base = obj match {
        case e: NodeJoined      => e.toJson.asJsObject
        case e: NodeLeft        => e.toJson.asJsObject
        case e: NodeUnreachable => e.toJson.asJsObject
        case e: NodeReachable   => e.toJson.asJsObject
        case e: ShardRebalanced => e.toJson.asJsObject
      }
      JsObject(base.fields + ("eventType" -> JsString(obj.eventType)))
    }

    override def read(json: JsValue): ClusterEvent = {
      val fields = json.asJsObject.fields
      fields("eventType") match {
        case JsString("NodeJoined")      => json.convertTo[NodeJoined]
        case JsString("NodeLeft")        => json.convertTo[NodeLeft]
        case JsString("NodeUnreachable") => json.convertTo[NodeUnreachable]
        case JsString("NodeReachable")   => json.convertTo[NodeReachable]
        case JsString("ShardRebalanced") => json.convertTo[ShardRebalanced]
        case other => spray.json.deserializationError(s"Unknown ClusterEvent type: $other")
      }
    }
  }

  /**
   * Diff two consecutive ClusterStatus snapshots and produce structured events.
   */
  def diff(previous: ClusterStatus, current: ClusterStatus, timestamp: Long): List[ClusterEvent] = {
    val prevAddresses = previous.nodes.map(_.address).toSet
    val currAddresses = current.nodes.map(_.address).toSet
    val prevByAddr    = previous.nodes.map(n => n.address -> n).toMap
    val currByAddr    = current.nodes.map(n => n.address -> n).toMap

    val joined = (currAddresses -- prevAddresses).toList.sorted.map { addr =>
      val node = currByAddr(addr)
      NodeJoined(timestamp, addr, node.roles)
    }

    val left = (prevAddresses -- currAddresses).toList.sorted.map { addr =>
      val node = prevByAddr(addr)
      NodeLeft(timestamp, addr, node.roles)
    }

    val unreachable = currAddresses.intersect(prevAddresses).toList.sorted.flatMap { addr =>
      val prev = prevByAddr(addr)
      val curr = currByAddr(addr)
      if (prev.status != "Unreachable" && curr.status == "Unreachable")
        Some(NodeUnreachable(timestamp, addr))
      else if (prev.status == "Unreachable" && curr.status != "Unreachable")
        Some(NodeReachable(timestamp, addr))
      else None
    }

    // Detect shard rebalances: shards that moved from one node to another
    val prevShardToNode = (for {
      node  <- previous.nodes
      shard <- node.shards
    } yield shard.shardId -> node.address).toMap

    val currShardToNode = (for {
      node  <- current.nodes
      shard <- node.shards
    } yield shard.shardId -> node.address).toMap

    val rebalanced = currShardToNode.toList.sorted.flatMap { case (shardId, currAddr) =>
      prevShardToNode.get(shardId) match {
        case Some(prevAddr) if prevAddr != currAddr =>
          Some(ShardRebalanced(timestamp, shardId, prevAddr, currAddr))
        case _ => None
      }
    }

    joined ++ left ++ unreachable ++ rebalanced
  }
}
