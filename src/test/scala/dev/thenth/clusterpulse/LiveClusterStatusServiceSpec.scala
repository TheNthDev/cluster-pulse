package dev.thenth.clusterpulse

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import dev.thenth.clusterpulse.ClusterStatusTracker.*
import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo, ShardInfo}

object LiveClusterStatusServiceSpec {
  val config = ConfigFactory
    .parseString(
      """
      |cluster-pulse {
      |  report-interval = 1s
      |  ask-timeout = 3s
      |  stream-interval = 500ms
      |  history-buffer-size = 10
      |  split-brain-membership-threshold = 0.5
      |}
      |""".stripMargin
    )
    .withFallback(ConfigFactory.load())
}

class LiveClusterStatusServiceSpec
    extends ScalaTestWithActorTestKit(LiveClusterStatusServiceSpec.config)
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  "LiveClusterStatusService" should {

    "return cluster status via getStatus" in {
      val expectedStatus = ClusterStatus(
        List(NodeInfo("pekko://test@127.0.0.1:2551", "Up", Set("core"), List(ShardInfo("s1", 2, List("e1", "e2"))))),
        2,
        List("e1", "e2")
      )

      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(expectedStatus)
          Behaviors.same
        case _ => Behaviors.same
      })

      val service = new LiveClusterStatusService(fakeTracker)
      val result  = service.getStatus.futureValue
      result shouldBe expectedStatus
    }

    "return empty cluster status" in {
      val emptyStatus = ClusterStatus(Nil, 0, Nil)

      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(emptyStatus)
          Behaviors.same
        case _ => Behaviors.same
      })

      val service = new LiveClusterStatusService(fakeTracker)
      val result  = service.getStatus.futureValue
      result shouldBe emptyStatus
      result.nodes shouldBe empty
      result.totalEntityCount shouldBe 0
    }

    "produce a status stream" in {
      val status = ClusterStatus(
        List(NodeInfo("pekko://test@127.0.0.1:2551", "Up", Set.empty, Nil)),
        0,
        Nil
      )

      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(status)
          Behaviors.same
        case _ => Behaviors.same
      })

      val service = new LiveClusterStatusService(fakeTracker)
      val stream  = service.statusStream()

      import org.apache.pekko.stream.scaladsl.Sink
      val results = stream.take(2).runWith(Sink.seq).futureValue
      results should have size 2
      results.foreach(_ shouldBe status)
    }

    "read settings from config" in {
      val fakeTracker = spawn(Behaviors.receiveMessage[Command] {
        case GetStatus(replyTo) =>
          replyTo ! ClusterStatusResponse(ClusterStatus(Nil, 0, Nil))
          Behaviors.same
        case _ => Behaviors.same
      })

      // Just verify construction doesn't throw
      noException should be thrownBy new LiveClusterStatusService(fakeTracker)
    }
  }
}
