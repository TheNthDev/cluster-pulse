package dev.thenth.clusterpulse
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.*
import dev.thenth.clusterpulse.model.ClusterStatus
import dev.thenth.clusterpulse.model.ClusterStatus.*
import dev.thenth.clusterpulse.model.{NodeInfo, ShardInfo}
class ClusterStatusModelSpec extends AnyWordSpec with Matchers {
  "ShardInfo" should {
    "round-trip through JSON" in {
      val shard = ShardInfo("shard-1", 3, List("e1", "e2", "e3"))
      shard.toJson.convertTo[ShardInfo] shouldBe shard
    }
    "report correct entityCount" in {
      ShardInfo("s", 5, List("a", "b", "c", "d", "e")).entityCount shouldBe 5
    }
  }
  "NodeInfo" should {
    "round-trip through JSON" in {
      val node = NodeInfo("pekko://sys@127.0.0.1:2551", "Up", Set("core"), List(ShardInfo("s1", 1, List("e1"))))
      node.toJson.convertTo[NodeInfo] shouldBe node
    }
    "preserve roles as a set" in {
      val node = NodeInfo("addr", "Up", Set("role-a", "role-b"), Nil)
      node.toJson.convertTo[NodeInfo].roles shouldBe Set("role-a", "role-b")
    }
  }
  "ClusterStatus" should {
    "round-trip through JSON with nodes and entities" in {
      val shard  = ShardInfo("shard-1", 2, List("e1", "e2"))
      val node   = NodeInfo("pekko://sys@127.0.0.1:2551", "Up", Set("core"), List(shard))
      val status = ClusterStatus(List(node), 2, List("e1", "e2"))
      status.toJson.convertTo[ClusterStatus] shouldBe status
    }
    "round-trip an empty ClusterStatus" in {
      val status = ClusterStatus(Nil, 0, Nil)
      status.toJson.convertTo[ClusterStatus] shouldBe status
    }
    "correctly count total entities across multiple nodes" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 3, List("a", "b", "c")))),
        NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s2", 2, List("d", "e"))))
      )
      val status = ClusterStatus(nodes, totalEntityCount = 5, activeEntities = List("a", "b", "c", "d", "e"))
      status.totalEntityCount shouldBe 5
      status.activeEntities should have size 5
    }
    "identify unreachable nodes by status field" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set.empty, Nil),
        NodeInfo("addr-2", "Unreachable", Set.empty, Nil),
        NodeInfo("addr-3", "Up", Set.empty, Nil)
      )
      val status = ClusterStatus(nodes, 0, Nil)
      status.nodes.count(_.status == "Unreachable") shouldBe 1
      status.nodes.count(_.status == "Up") shouldBe 2
    }
    "return zero unreachable when all nodes are Up" in {
      val nodes  = List(NodeInfo("addr-1", "Up", Set.empty, Nil))
      val status = ClusterStatus(nodes, 0, Nil)
      status.nodes.count(_.status == "Unreachable") shouldBe 0
    }
    "aggregate shard count across all nodes" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 1, List("e1")), ShardInfo("s2", 1, List("e2")))),
        NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s3", 1, List("e3"))))
      )
      val status = ClusterStatus(nodes, 3, List("e1", "e2", "e3"))
      status.nodes.flatMap(_.shards) should have size 3
    }
  }

  "ClusterStatus.unreachableCount" should {
    "return 0 when all nodes are Up" in {
      val status = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      status.unreachableCount shouldBe 0
    }
    "count unreachable nodes" in {
      val status = ClusterStatus(List(
        NodeInfo("a", "Up", Set.empty, Nil),
        NodeInfo("b", "Unreachable", Set.empty, Nil),
        NodeInfo("c", "Unreachable", Set.empty, Nil)
      ), 0, Nil)
      status.unreachableCount shouldBe 2
    }
  }

  "ClusterStatus.isHealthy" should {
    "return false for empty cluster" in {
      ClusterStatus(Nil, 0, Nil).isHealthy shouldBe false
    }
    "return true when all nodes are Up" in {
      val status = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      status.isHealthy shouldBe true
    }
    "return false when any node is Unreachable" in {
      val status = ClusterStatus(List(
        NodeInfo("a", "Up", Set.empty, Nil),
        NodeInfo("b", "Unreachable", Set.empty, Nil)
      ), 0, Nil)
      status.isHealthy shouldBe false
    }
  }

  "ClusterStatus.healthScore" should {
    "return 0 for empty cluster" in {
      ClusterStatus(Nil, 0, Nil).healthScore shouldBe 0
    }
    "return 100 for a healthy, balanced cluster" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 5, Nil))),
        NodeInfo("b", "Up", Set.empty, List(ShardInfo("s2", 5, Nil)))
      )
      ClusterStatus(nodes, 10, Nil).healthScore shouldBe 100
    }
    "deduct points for unreachable nodes" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 5, Nil))),
        NodeInfo("b", "Unreachable", Set.empty, List(ShardInfo("s2", 5, Nil)))
      )
      val score = ClusterStatus(nodes, 10, Nil).healthScore
      score should be < 100
      score should be > 0
    }
    "return 100 for single healthy node with no entities" in {
      val status = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      status.healthScore shouldBe 100
    }
  }

  "ClusterStatus.shardBalanceScore" should {
    "return 0.0 for empty cluster" in {
      ClusterStatus(Nil, 0, Nil).shardBalanceScore shouldBe 0.0
    }
    "return 0.0 for single node" in {
      val status = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 10, Nil)))), 10, Nil)
      status.shardBalanceScore shouldBe 0.0
    }
    "return 0.0 for perfectly balanced nodes" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 5, Nil))),
        NodeInfo("b", "Up", Set.empty, List(ShardInfo("s2", 5, Nil)))
      )
      ClusterStatus(nodes, 10, Nil).shardBalanceScore shouldBe 0.0
    }
    "return > 0 for imbalanced nodes" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 10, Nil))),
        NodeInfo("b", "Up", Set.empty, List(ShardInfo("s2", 0, Nil)))
      )
      ClusterStatus(nodes, 10, Nil).shardBalanceScore should be > 0.0
    }
    "return 0.0 when all nodes have zero entities" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, Nil),
        NodeInfo("b", "Up", Set.empty, Nil)
      )
      ClusterStatus(nodes, 0, Nil).shardBalanceScore shouldBe 0.0
    }
    "be clamped to at most 1.0" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 1000, Nil))),
        NodeInfo("b", "Up", Set.empty, List(ShardInfo("s2", 0, Nil)))
      )
      ClusterStatus(nodes, 1000, Nil).shardBalanceScore should be <= 1.0
    }
    "exclude non-hosting nodes (no shards) from balance calculation" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 10, Nil))),
        NodeInfo("b", "Up", Set.empty, Nil) // non-hosting node, no shards
      )
      // Only one hosting node → returns 0.0 (fewer than 2 hosting nodes)
      ClusterStatus(nodes, 10, Nil).shardBalanceScore shouldBe 0.0
    }
    "compute balance only across shard-hosting nodes" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 5, Nil))),
        NodeInfo("b", "Up", Set.empty, List(ShardInfo("s2", 5, Nil))),
        NodeInfo("c", "Up", Set.empty, Nil) // non-hosting node
      )
      // Two hosting nodes with equal entities → perfectly balanced
      ClusterStatus(nodes, 10, Nil).shardBalanceScore shouldBe 0.0
    }
  }

  "ClusterStatus.healthScore with non-hosting nodes" should {
    "return 100 when only one node hosts shards and others are non-hosting" in {
      val nodes = List(
        NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 15, Nil))),
        NodeInfo("b", "Up", Set.empty, Nil) // non-hosting
      )
      ClusterStatus(nodes, 15, Nil).healthScore shouldBe 100
    }
  }
}
