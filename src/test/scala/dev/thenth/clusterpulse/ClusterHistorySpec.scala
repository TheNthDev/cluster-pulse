package dev.thenth.clusterpulse

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import dev.thenth.clusterpulse.model.{ClusterEvent, ClusterStatus, NodeInfo, ShardInfo}

class ClusterHistorySpec extends AnyWordSpec with Matchers {

  private def mkStatus(nodeAddrs: String*): ClusterStatus =
    ClusterStatus(nodeAddrs.map(a => NodeInfo(a, "Up", Set.empty, Nil)).toList, 0, Nil)

  "ClusterHistory" should {
    "start empty" in {
      val h = new ClusterHistory(10)
      h.size shouldBe 0
      h.snapshots shouldBe empty
      h.latest shouldBe None
      h.events shouldBe empty
    }

    "record a single snapshot" in {
      val h = new ClusterHistory(10)
      val s = mkStatus("a")
      h.record(s)
      h.size shouldBe 1
      h.latest shouldBe Some(s)
    }

    "emit no events on first snapshot" in {
      val h      = new ClusterHistory(10)
      val events = h.record(mkStatus("a"))
      events shouldBe empty
    }

    "emit events when nodes change" in {
      val h = new ClusterHistory(10)
      h.record(mkStatus("a"))
      val events = h.record(mkStatus("a", "b"))
      events.collect { case e: ClusterEvent.NodeJoined => e } should have size 1
    }

    "retain events across multiple snapshots" in {
      val h = new ClusterHistory(10)
      h.record(mkStatus("a"))
      h.record(mkStatus("a", "b"))
      h.record(mkStatus("a"))
      h.events.size should be >= 2
    }

    "evict oldest snapshots when buffer is full" in {
      val h = new ClusterHistory(3)
      h.record(mkStatus("a"))
      h.record(mkStatus("a", "b"))
      h.record(mkStatus("a", "b", "c"))
      h.record(mkStatus("a", "b", "c", "d"))
      h.size shouldBe 3
      h.snapshots.head.nodes.map(_.address) should contain("b")
    }

    "compute nodeCountDelta correctly" in {
      val h = new ClusterHistory(10)
      h.nodeCountDelta shouldBe 0
      h.record(mkStatus("a"))
      h.nodeCountDelta shouldBe 0
      h.record(mkStatus("a", "b"))
      h.nodeCountDelta shouldBe 1
      h.record(mkStatus("a", "b", "c"))
      h.nodeCountDelta shouldBe 2
    }

    "compute negative nodeCountDelta when nodes leave" in {
      val h = new ClusterHistory(10)
      h.record(mkStatus("a", "b", "c"))
      h.record(mkStatus("a"))
      h.nodeCountDelta shouldBe -2
    }

    "be thread-safe under concurrent writes" in {
      val h = new ClusterHistory(100)
      val threads = (1 to 10).map { i =>
        new Thread(() =>
          (1 to 20).foreach { j =>
            h.record(mkStatus(s"node-$i-$j"))
          }
        )
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
      h.size shouldBe 100
    }
  }
}
