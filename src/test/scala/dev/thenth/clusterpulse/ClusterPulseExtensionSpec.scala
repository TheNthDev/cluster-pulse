package dev.thenth.clusterpulse

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

object ClusterPulseExtensionSpec {
  val config = ConfigFactory.parseString(
    """
      |pekko.actor.provider = cluster
      |pekko.remote.artery.canonical.port = 0
      |pekko.remote.artery.canonical.hostname = 127.0.0.1
      |""".stripMargin
  ).withFallback(ConfigFactory.load())
}

class ClusterPulseExtensionSpec
  extends ScalaTestWithActorTestKit(ClusterPulseExtensionSpec.config)
  with AnyWordSpecLike
  with Matchers {

  "ClusterPulse Extension" should {

    "be accessible via ClusterPulse(system)" in {
      val pulse = ClusterPulse(system)
      pulse should not be null
    }

    "return the same instance on repeated access (singleton)" in {
      val pulse1 = ClusterPulse(system)
      val pulse2 = ClusterPulse(system)
      pulse1 shouldBe theSameInstanceAs(pulse2)
    }

    "expose tracker, service, history, and splitBrainDetector" in {
      val pulse = ClusterPulse(system)
      pulse.tracker should not be null
      pulse.service should not be null
      pulse.history should not be null
      pulse.splitBrainDetector should not be null
    }

    "support registerEntity without error" in {
      import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
      val pulse = ClusterPulse(system)
      val typeKey = EntityTypeKey[String]("ext-test-type")
      noException should be thrownBy pulse.registerEntity(typeKey)
    }
  }
}
