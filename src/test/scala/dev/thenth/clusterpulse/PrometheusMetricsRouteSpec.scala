package dev.thenth.clusterpulse

import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo, ShardInfo}

class PrometheusMetricsRouteSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private val twoNodeStatus = ClusterStatus(
    List(
      NodeInfo("a", "Up", Set.empty, List(ShardInfo("s1", 3, Nil))),
      NodeInfo("b", "Unreachable", Set.empty, List(ShardInfo("s2", 2, Nil)))
    ),
    5,
    Nil
  )

  "PrometheusMetricsRoute" should {
    "return 200 on GET /metrics" in {
      val route = PrometheusMetricsRoute(() => twoNodeStatus).route
      Get("/metrics") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val body = responseAs[String]
        body should include("cluster_pulse_node_count 2")
        body should include("cluster_pulse_node_unreachable 1")
        body should include("cluster_pulse_entity_count 5")
        body should include("cluster_pulse_shard_count 2")
      }
    }

    "include per-node metrics" in {
      val route = PrometheusMetricsRoute(() => twoNodeStatus).route
      Get("/metrics") ~> route ~> check {
        val body = responseAs[String]
        body should include("""cluster_pulse_node_entity_count{node_address="a"} 3""")
        body should include("""cluster_pulse_node_entity_count{node_address="b"} 2""")
        body should include("""cluster_pulse_node_status{node_address="a"} 1""")
        body should include("""cluster_pulse_node_status{node_address="b"} 0""")
      }
    }

    "include health score and shard balance" in {
      val route = PrometheusMetricsRoute(() => twoNodeStatus).route
      Get("/metrics") ~> route ~> check {
        val body = responseAs[String]
        body should include("cluster_pulse_health_score")
        body should include("cluster_pulse_shard_balance")
      }
    }

    "include split-brain gauge when detector is provided" in {
      val sbd = new SplitBrainDetector()
      sbd.update(twoNodeStatus) // 1 of 2 unreachable = majority
      val route = PrometheusMetricsRoute(() => twoNodeStatus, Some(sbd)).route
      Get("/metrics") ~> route ~> check {
        val body = responseAs[String]
        body should include("cluster_pulse_split_brain_detected 1")
      }
    }

    "include history delta when history is provided" in {
      val h = new ClusterHistory(10)
      h.record(ClusterStatus(List(NodeInfo("a", "Up", Set.empty, Nil)), 0, Nil))
      h.record(twoNodeStatus)
      val route = PrometheusMetricsRoute(() => twoNodeStatus, history = Some(h)).route
      Get("/metrics") ~> route ~> check {
        val body = responseAs[String]
        body should include("cluster_pulse_node_count_delta 1")
      }
    }

    "return correct content type" in {
      val route = PrometheusMetricsRoute(() => ClusterStatus(Nil, 0, Nil)).route
      Get("/metrics") ~> route ~> check {
        status shouldBe StatusCodes.OK
        contentType.toString should include("text/plain")
      }
    }
  }
}
