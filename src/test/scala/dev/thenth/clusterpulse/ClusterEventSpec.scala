package dev.thenth.clusterpulse

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.*

import dev.thenth.clusterpulse.model.{ClusterEvent, ClusterStatus, NodeInfo, ShardInfo}
import dev.thenth.clusterpulse.model.ClusterEvent.*

class ClusterEventSpec extends AnyWordSpec with Matchers {

  val ts = 1000L

  "ClusterEvent.diff" should {
    "detect a node joining" in {
      val prev = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      val curr = ClusterStatus(
        List(
          NodeInfo("a", "Up", Set.empty, Nil),
          NodeInfo("b", "Up", Set("core"), Nil)
        ),
        0,
        Nil
      )
      val events = ClusterEvent.diff(prev, curr, ts)
      events should contain(NodeJoined(ts, "b", Set("core")))
      events.collect { case e: NodeJoined => e } should have size 1
    }

    "detect a node leaving" in {
      val prev = ClusterStatus(
        List(
          NodeInfo("a", "Up", Set.empty, Nil),
          NodeInfo("b", "Up", Set("core"), Nil)
        ),
        0,
        Nil
      )
      val curr   = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      val events = ClusterEvent.diff(prev, curr, ts)
      events should contain(NodeLeft(ts, "b", Set("core")))
    }

    "detect a node becoming unreachable" in {
      val prev   = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      val curr   = ClusterStatus(List(NodeInfo("a", "Unreachable", Set.empty, Nil)), 0, Nil)
      val events = ClusterEvent.diff(prev, curr, ts)
      events should contain(NodeUnreachable(ts, "a"))
    }

    "detect a node becoming reachable again" in {
      val prev   = ClusterStatus(List(NodeInfo("a", "Unreachable", Set.empty, Nil)), 0, Nil)
      val curr   = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      val events = ClusterEvent.diff(prev, curr, ts)
      events should contain(NodeReachable(ts, "a"))
    }

    "detect shard rebalance" in {
      val prev = ClusterStatus(
        List(
          NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 1, List("e1")))),
          NodeInfo("b", "Up", Set.empty, Nil)
        ),
        1,
        List("e1")
      )
      val curr = ClusterStatus(
        List(
          NodeInfo("a", "Up", Set.empty, Nil),
          NodeInfo("b", "Up", Set.empty, List(ShardInfo("s1", 1, List("e1"))))
        ),
        1,
        List("e1")
      )
      val events = ClusterEvent.diff(prev, curr, ts)
      events should contain(ShardRebalanced(ts, "s1", "a", "b"))
    }

    "return empty list when nothing changed" in {
      val status = ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil)
      ClusterEvent.diff(status, status, ts) shouldBe empty
    }

    "detect multiple changes at once" in {
      val prev = ClusterStatus(
        List(
          NodeInfo("a", "Up", Set.empty, Nil),
          NodeInfo("b", "Up", Set.empty, Nil)
        ),
        0,
        Nil
      )
      val curr = ClusterStatus(
        List(
          NodeInfo("a", "Unreachable", Set.empty, Nil),
          NodeInfo("c", "Up", Set("gw"), Nil)
        ),
        0,
        Nil
      )
      val events = ClusterEvent.diff(prev, curr, ts)
      events.collect { case e: NodeLeft => e } should have size 1
      events.collect { case e: NodeJoined => e } should have size 1
      events.collect { case e: NodeUnreachable => e } should have size 1
    }
  }

  "ClusterEvent JSON" should {
    "round-trip NodeJoined" in {
      val e: ClusterEvent = NodeJoined(ts, "a", Set("core"))
      val json            = e.toJson
      json.convertTo[ClusterEvent] shouldBe e
    }

    "round-trip NodeLeft" in {
      val e: ClusterEvent = NodeLeft(ts, "a", Set("gw"))
      e.toJson.convertTo[ClusterEvent] shouldBe e
    }

    "round-trip NodeUnreachable" in {
      val e: ClusterEvent = NodeUnreachable(ts, "a")
      e.toJson.convertTo[ClusterEvent] shouldBe e
    }

    "round-trip NodeReachable" in {
      val e: ClusterEvent = NodeReachable(ts, "a")
      e.toJson.convertTo[ClusterEvent] shouldBe e
    }

    "round-trip ShardRebalanced" in {
      val e: ClusterEvent = ShardRebalanced(ts, "s1", "a", "b")
      e.toJson.convertTo[ClusterEvent] shouldBe e
    }

    "fail on unknown event type" in {
      import spray.json.*
      val json = """{"eventType":"UnknownType","timestamp":1000}""".parseJson
      a[spray.json.DeserializationException] should be thrownBy json.convertTo[ClusterEvent]
    }
  }
}
