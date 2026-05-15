package dev.thenth.clusterpulse

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import dev.thenth.clusterpulse.ClusterStatusTracker.*
import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo, ShardInfo}

object StandaloneClusterPulseSpec {
  val config = ConfigFactory.parseString(
    """
      |pekko.actor.provider = cluster
      |pekko.remote.artery.canonical.port = 0
      |pekko.remote.artery.canonical.hostname = 127.0.0.1
      |""".stripMargin
  ).withFallback(ConfigFactory.load())
}

class StandaloneClusterPulseSpec
  extends ScalaTestWithActorTestKit(StandaloneClusterPulseSpec.config)
  with AnyWordSpecLike
  with Matchers
  with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  "StandaloneClusterPulse" should {

    "return status via the service" in {
      val expectedStatus = ClusterStatus(
        List(NodeInfo("pekko://test@127.0.0.1:2551", "Up", Set("core"), Nil)),
        0, Nil
      )

      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(expectedStatus)
          Behaviors.same
        case _ => Behaviors.same
      })

      val service = new LiveClusterStatusService(fakeTracker)
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val pulse = new StandaloneClusterPulse(fakeTracker, service, None, None)(using system, sharding)

      val result = pulse.status.futureValue
      result shouldBe expectedStatus
    }

    "produce a status stream" in {
      val status = ClusterStatus(Nil, 0, Nil)

      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(status)
          Behaviors.same
        case _ => Behaviors.same
      })

      val service = new LiveClusterStatusService(fakeTracker)
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val pulse = new StandaloneClusterPulse(fakeTracker, service, None, None)(using system, sharding)

      import org.apache.pekko.stream.scaladsl.Sink
      val results = pulse.statusStream().take(1).runWith(Sink.seq).futureValue
      results should have size 1
      results.head shouldBe status
    }

    "expose history and splitBrainDetector when provided" in {
      val fakeTracker = spawn(Behaviors.receiveMessage[Command] { _ => Behaviors.same })
      val service = new LiveClusterStatusService(fakeTracker)
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])

      val history = new ClusterHistory(10)
      val sbd = new SplitBrainDetector()
      val pulse = new StandaloneClusterPulse(fakeTracker, service, Some(history), Some(sbd))(using system, sharding)

      pulse.history shouldBe Some(history)
      pulse.splitBrainDetector shouldBe Some(sbd)
    }

    "expose None for history and splitBrainDetector when not provided" in {
      val fakeTracker = spawn(Behaviors.receiveMessage[Command] { _ => Behaviors.same })
      val service = new LiveClusterStatusService(fakeTracker)
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val pulse = new StandaloneClusterPulse(fakeTracker, service, None, None)(using system, sharding)

      pulse.history shouldBe None
      pulse.splitBrainDetector shouldBe None
    }

    "create with single type key factory" in {
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val typeKey = EntityTypeKey[String]("standalone-single-key")
      val pulse = ClusterPulse.create(system, sharding, typeKey, None, None, None, None)
      pulse should not be null
      pulse.tracker should not be null
    }

    "create with history and split-brain detector" in {
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val history = new ClusterHistory(20)
      val sbd = new SplitBrainDetector(0.3)
      val pulse = ClusterPulse.create(system, sharding, reporter = None, history = Some(history), splitBrainDetector = Some(sbd))
      pulse should not be null
      pulse.history shouldBe Some(history)
      pulse.splitBrainDetector shouldBe Some(sbd)
    }
  }
}
