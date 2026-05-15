package dev.thenth.clusterpulse

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.*

class ClusterPulseSettingsSpec extends AnyWordSpec with Matchers {

  "ClusterPulseSettings" should {
    "load defaults from reference.conf" in {
      val config   = ConfigFactory.load()
      val settings = ClusterPulseSettings(config)
      settings.reportInterval shouldBe 15.seconds
      settings.askTimeout.duration shouldBe 5.seconds
      settings.streamInterval shouldBe 2.seconds
      settings.historyBufferSize shouldBe 60
      settings.splitBrainMembershipThreshold shouldBe 0.5
      settings.includeEntityIds shouldBe true
    }

    "allow overriding via application config" in {
      val overrides = ConfigFactory
        .parseString(
          """
          |cluster-pulse {
          |  report-interval = 30s
          |  ask-timeout = 10s
          |  stream-interval = 5s
          |  history-buffer-size = 120
          |  split-brain-membership-threshold = 0.3
          |}
          |""".stripMargin
        )
        .withFallback(ConfigFactory.load())
      val settings = ClusterPulseSettings(overrides)
      settings.reportInterval shouldBe 30.seconds
      settings.askTimeout.duration shouldBe 10.seconds
      settings.streamInterval shouldBe 5.seconds
      settings.historyBufferSize shouldBe 120
      settings.splitBrainMembershipThreshold shouldBe 0.3
      settings.includeEntityIds shouldBe true
    }

    "allow disabling entity ID enumeration" in {
      val overrides = ConfigFactory
        .parseString(
          """
          |cluster-pulse {
          |  include-entity-ids = false
          |}
          |""".stripMargin
        )
        .withFallback(ConfigFactory.load())
      val settings = ClusterPulseSettings(overrides)
      settings.includeEntityIds shouldBe false
    }
  }
}
