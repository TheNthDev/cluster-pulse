package dev.thenth.clusterpulse

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.apache.pekko.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import org.apache.pekko.cluster.sharding.ShardRegion.{CurrentShardRegionState, ShardState}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import dev.thenth.clusterpulse.model.*
import dev.thenth.clusterpulse.model.ClusterStatus.*
import spray.json.*
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader

object ClusterStatusTrackerSpec {
  val config = ConfigFactory.parseString(
    """
      |pekko {
      |  actor {
      |    provider = cluster
      |    serialization-bindings {
      |      "dev.thenth.clusterpulse.ClusterPulseSerializable" = jackson-cbor
      |    }
      |  }
      |  remote.artery {
      |    canonical.hostname = "127.0.0.1"
      |    canonical.port = 0
      |  }
      |  cluster {
      |    seed-nodes = []
      |    jmx.multi-mbeans-in-same-jvm = on
      |    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
      |  }
      |}
      |cluster-pulse {
      |  report-interval = 1s
      |  ask-timeout = 3s
      |  stream-interval = 500ms
      |  history-buffer-size = 10
      |  split-brain-membership-threshold = 0.5
      |  include-entity-ids = true
      |}
      |""".stripMargin
  ).withFallback(ConfigFactory.load())
}

class ClusterStatusTrackerSpec
  extends ScalaTestWithActorTestKit(ClusterStatusTrackerSpec.config)
  with AnyWordSpecLike
  with Matchers
  with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  "ClusterStatusTracker protocol types" should {

    "ClusterStatusResponse wraps a ClusterStatus" in {
      val status   = ClusterStatus(Nil, 0, Nil)
      val response = ClusterStatusTracker.ClusterStatusResponse(status)
      response.status shouldBe status
    }

    "ClusterStatusResponse preserves populated status" in {
      val shard  = ShardInfo("shard-1", 2, List("e1", "e2"))
      val node   = NodeInfo("pekko://sys@127.0.0.1:2551", "Up", Set("core"), List(shard))
      val status = ClusterStatus(List(node), 2, List("e1", "e2"))

      val response = ClusterStatusTracker.ClusterStatusResponse(status)
      response.status.nodes should have size 1
      response.status.totalEntityCount shouldBe 2
      response.status.activeEntities shouldBe List("e1", "e2")
    }

    "GetStatus command is serializable marker" in {
      val probe = org.apache.pekko.actor.testkit.typed.scaladsl.TestInbox[ClusterStatusTracker.Response]()
      val cmd   = ClusterStatusTracker.GetStatus(probe.ref)
      cmd shouldBe a[ClusterPulseSerializable]
    }

    "GetLocalState command is serializable marker" in {
      val probe = org.apache.pekko.actor.testkit.typed.scaladsl.TestInbox[ClusterStatusTracker.LocalStateResponse]()
      val cmd   = ClusterStatusTracker.GetLocalState(probe.ref)
      cmd shouldBe a[ClusterPulseSerializable]
    }

    "ClusterStatusResponse is serializable marker" in {
      val response = ClusterStatusTracker.ClusterStatusResponse(ClusterStatus(Nil, 0, Nil))
      response shouldBe a[ClusterPulseSerializable]
    }

    "ClusterStatusTrackerKey has expected name" in {
      ClusterStatusTracker.ClusterStatusTrackerKey.id shouldBe "ClusterStatusTracker"
    }

    "RegisterTypeKey command wraps an EntityTypeKey" in {
      val typeKey = EntityTypeKey[String]("test-entity")
      val cmd     = ClusterStatusTracker.RegisterTypeKey(typeKey)
      cmd shouldBe a[ClusterPulseSerializable]
      cmd.typeKey shouldBe typeKey
    }

    "LocalStateResponse carries address and shard region state" in {
      val shardState = CurrentShardRegionState(Set(ShardState("shard-1", Set("e1", "e2"))))
      val response   = ClusterStatusTracker.LocalStateResponse("pekko://sys@127.0.0.1:2551", shardState)
      response shouldBe a[ClusterPulseSerializable]
      response.address shouldBe "pekko://sys@127.0.0.1:2551"
      response.state.shards should have size 1
      response.state.shards.head.shardId shouldBe "shard-1"
      response.state.shards.head.entityIds shouldBe Set("e1", "e2")
    }

    "LocalStateResponse with empty shard state" in {
      val response = ClusterStatusTracker.LocalStateResponse("pekko://sys@127.0.0.1:2551", CurrentShardRegionState(Set.empty))
      response.state.shards shouldBe empty
    }

    "LocalStateResponse with multiple shards" in {
      val shardState = CurrentShardRegionState(Set(
        ShardState("shard-1", Set("e1")),
        ShardState("shard-2", Set("e2", "e3")),
        ShardState("shard-3", Set.empty)
      ))
      val response = ClusterStatusTracker.LocalStateResponse("addr-1", shardState)
      response.state.shards should have size 3
      response.state.shards.flatMap(_.entityIds) should have size 3
    }

    "ClusterStatusResponse with multiple nodes and shards" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set("core"), List(ShardInfo("s1", 2, List("e1", "e2")))),
        NodeInfo("addr-2", "Up", Set("worker"), List(ShardInfo("s2", 3, List("e3", "e4", "e5")))),
        NodeInfo("addr-3", "Unreachable", Set.empty, Nil)
      )
      val status   = ClusterStatus(nodes, 5, List("e1", "e2", "e3", "e4", "e5"))
      val response = ClusterStatusTracker.ClusterStatusResponse(status)
      response.status.nodes should have size 3
      response.status.totalEntityCount shouldBe 5
      response.status.unreachableCount shouldBe 1
      response.status.isHealthy shouldBe false
    }

    "ClusterStatusResponse healthScore reflects unreachable nodes" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set.empty, Nil),
        NodeInfo("addr-2", "Unreachable", Set.empty, Nil)
      )
      val status   = ClusterStatus(nodes, 0, Nil)
      val response = ClusterStatusTracker.ClusterStatusResponse(status)
      response.status.healthScore should be < 100
    }

    "RegisterTypeKey with different entity type keys" in {
      val typeKey1 = EntityTypeKey[String]("entity-a")
      val typeKey2 = EntityTypeKey[Int]("entity-b")
      val cmd1     = ClusterStatusTracker.RegisterTypeKey(typeKey1)
      val cmd2     = ClusterStatusTracker.RegisterTypeKey(typeKey2)
      cmd1.typeKey should not be cmd2.typeKey
      cmd1.typeKey.name shouldBe "entity-a"
      cmd2.typeKey.name shouldBe "entity-b"
    }
  }

  "ClusterStatusTracker actor behavior" should {

    "spawn with empty type keys and respond to GetStatus" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, None, None, None))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      val response = probe.receiveMessage()
      response shouldBe a[ClusterStatusTracker.ClusterStatusResponse]
    }

    "spawn with single type key via backward-compatible apply" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val typeKey = EntityTypeKey[String]("single-key-test")
      val tracker = spawn(ClusterStatusTracker(sharding, typeKey))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      val response = probe.receiveMessage()
      response shouldBe a[ClusterStatusTracker.ClusterStatusResponse]
    }

    "accept RegisterTypeKey and continue operating" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, None, None, None))
      val typeKey = EntityTypeKey[String]("dynamic-key")
      tracker ! ClusterStatusTracker.RegisterTypeKey(typeKey)
      // Verify the actor is still alive by sending GetStatus
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      probe.receiveMessage() shouldBe a[ClusterStatusTracker.ClusterStatusResponse]
    }

    "update reporter when provided" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val reporter = OtelClusterReporter(OpenTelemetry.noop())
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, Some(reporter), None, None, None))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      val response = probe.receiveMessage()
      response shouldBe a[ClusterStatusTracker.ClusterStatusResponse]
    }

    "update history when provided" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val history = ClusterHistory(maxSize = 10)
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, Some(history), None, None))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      probe.receiveMessage()
      eventually {
        history.size should be > 0
      }
    }

    "update split-brain detector when provided" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val sbd = SplitBrainDetector()
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, None, Some(sbd), None))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      probe.receiveMessage()
      // SBD should have been updated (not detected since single node is Up)
      eventually {
        sbd.isDetected shouldBe false
      }
    }

    "update all optional components together" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val reporter = OtelClusterReporter(OpenTelemetry.noop())
      val history = ClusterHistory(maxSize = 10)
      val sbd = SplitBrainDetector()
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, Some(reporter), Some(history), Some(sbd), None))
      val probe = TestProbe[ClusterStatusTracker.Response]()
      tracker ! ClusterStatusTracker.GetStatus(probe.ref)
      probe.receiveMessage() shouldBe a[ClusterStatusTracker.ClusterStatusResponse]
      eventually {
        history.size should be > 0
      }
    }

    "respond to GetLocalState with empty type keys" in {
      val sharding = org.mockito.Mockito.mock(classOf[ClusterSharding])
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, None, None, None))
      val probe = TestProbe[ClusterStatusTracker.LocalStateResponse]()
      tracker ! ClusterStatusTracker.GetLocalState(probe.ref)
      val response = probe.receiveMessage()
      response.address should not be empty
      response.state.shards shouldBe empty
    }
  }

  "ClusterStatus JSON round-trip via tracker model" should {

    "round-trip a ClusterStatus with nodes and entities" in {
      val shard  = ShardInfo("shard-1", 2, List("e1", "e2"))
      val node   = NodeInfo("pekko://sys@127.0.0.1:2551", "Up", Set("core"), List(shard))
      val status = ClusterStatus(List(node), 2, List("e1", "e2"))

      val json = status.toJson.compactPrint
      val back = json.parseJson.convertTo[ClusterStatus]
      back shouldBe status
    }

    "round-trip an empty ClusterStatus" in {
      val status = ClusterStatus(Nil, 0, Nil)
      val json   = status.toJson.compactPrint
      val back   = json.parseJson.convertTo[ClusterStatus]
      back shouldBe status
    }

    "round-trip a ClusterStatus with multiple nodes and mixed statuses" in {
      val nodes = List(
        NodeInfo("addr-1", "Up", Set("core", "worker"), List(ShardInfo("s1", 3, List("e1", "e2", "e3")))),
        NodeInfo("addr-2", "Unreachable", Set.empty, Nil),
        NodeInfo("addr-3", "Up", Set("worker"), List(ShardInfo("s2", 1, List("e4")), ShardInfo("s3", 0, Nil)))
      )
      val status = ClusterStatus(nodes, 4, List("e1", "e2", "e3", "e4"))
      val json   = status.toJson.compactPrint
      val back   = json.parseJson.convertTo[ClusterStatus]
      back shouldBe status
    }

    "round-trip a ClusterStatus with nodes but no entities" in {
      val nodes  = List(NodeInfo("addr-1", "Up", Set.empty, Nil))
      val status = ClusterStatus(nodes, 0, Nil)
      val json   = status.toJson.compactPrint
      val back   = json.parseJson.convertTo[ClusterStatus]
      back shouldBe status
      back.nodes should have size 1
      back.totalEntityCount shouldBe 0
    }
  }
}
