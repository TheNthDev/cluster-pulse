package dev.thenth.clusterpulse

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo}

class SplitBrainDetectorSpec extends AnyWordSpec with Matchers {

  private def mkStatus(nodes: (String, String)*): ClusterStatus =
    ClusterStatus(nodes.map { case (a, s) => NodeInfo(a, s, Set.empty, Nil) }.toList, 0, Nil)

  "SplitBrainDetector" should {
    "not detect split-brain for empty cluster" in {
      val d = new SplitBrainDetector()
      d.update(ClusterStatus(Nil, 0, Nil)) shouldBe false
      d.isDetected shouldBe false
    }

    "not detect split-brain for healthy cluster" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Up", "b" -> "Up", "c" -> "Up")) shouldBe false
      d.isDetected shouldBe false
    }

    "detect unreachable majority (2 of 3)" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Up", "b" -> "Unreachable", "c" -> "Unreachable")) shouldBe true
      d.isDetected shouldBe true
      d.reason.get should include("Unreachable majority")
    }

    "detect unreachable majority (1 of 2)" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Up", "b" -> "Unreachable")) shouldBe true
    }

    "not detect when minority is unreachable (1 of 3)" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Up", "b" -> "Up", "c" -> "Unreachable")) shouldBe false
    }

    "detect rapid membership change" in {
      val d = new SplitBrainDetector(membershipChangeThreshold = 0.5)
      d.update(mkStatus("a" -> "Up", "b" -> "Up", "c" -> "Up", "d" -> "Up"))
      // 3 of 4 nodes disappear, 3 new ones appear = 6 changes out of 4 total
      val result = d.update(mkStatus("e" -> "Up", "f" -> "Up", "g" -> "Up", "a" -> "Up"))
      result shouldBe true
      d.reason.get should include("Rapid membership change")
    }

    "not detect rapid change when below threshold" in {
      val d = new SplitBrainDetector(membershipChangeThreshold = 0.6)
      d.update(mkStatus("a" -> "Up", "b" -> "Up", "c" -> "Up", "d" -> "Up"))
      d.update(mkStatus("a" -> "Up", "b" -> "Up", "c" -> "Up", "e" -> "Up")) shouldBe false
    }

    "detect no Up nodes (all Joining)" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Joining", "b" -> "Joining")) shouldBe true
      d.reason.get should include("No Up nodes")
    }

    "clear detection when cluster recovers" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Up", "b" -> "Unreachable")) shouldBe true
      d.update(mkStatus("a" -> "Up", "b" -> "Up")) shouldBe false
      d.isDetected shouldBe false
      d.reason shouldBe None
    }

    "clear detection when empty cluster follows split-brain" in {
      val d = new SplitBrainDetector()
      d.update(mkStatus("a" -> "Joining")) shouldBe true
      d.update(ClusterStatus(Nil, 0, Nil)) shouldBe false
    }
  }
}
