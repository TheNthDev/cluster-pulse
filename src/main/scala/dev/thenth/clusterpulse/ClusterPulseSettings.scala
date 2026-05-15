package dev.thenth.clusterpulse

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import org.apache.pekko.util.Timeout

import java.time.{Duration as JDuration}
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters.*

/**
 * Typed accessor for all `cluster-pulse.*` configuration values.
 *
 * Reads from the supplied Typesafe `Config` and exposes each setting as a
 * strongly-typed Scala value (durations, timeouts, thresholds, etc.).
 *
 * @param config the root application config (must contain a `cluster-pulse` section)
 */
class ClusterPulseSettings(config: Config) {
  private val cp = config.getConfig("cluster-pulse")

  val reportInterval: FiniteDuration = cp.getDuration("report-interval").toScala
  val askTimeout: Timeout            = Timeout(cp.getDuration("ask-timeout").toScala)
  val streamInterval: FiniteDuration = cp.getDuration("stream-interval").toScala
  val historyBufferSize: Int         = cp.getInt("history-buffer-size")
  val splitBrainMembershipThreshold: Double = cp.getDouble("split-brain-membership-threshold")
  val includeEntityIds: Boolean             = cp.getBoolean("include-entity-ids")
}

object ClusterPulseSettings {
  def apply(config: Config): ClusterPulseSettings = new ClusterPulseSettings(config)
}
