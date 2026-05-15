package dev.thenth.clusterpulse

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo, ShardInfo}

class OtelClusterReporterSpec extends AnyWordSpec with Matchers {

  private def noopReporter: OtelClusterReporter = OtelClusterReporter(OpenTelemetry.noop())

  /** Creates an OtelClusterReporter backed by an in-memory metric reader so gauge callbacks are actually invoked. */
  private def sdkReporter(
    sbd: Option[SplitBrainDetector] = None,
    history: Option[ClusterHistory] = None
  ): (OtelClusterReporter, InMemoryMetricReader) = {
    val reader        = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val sdk           = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build()
    val reporter      = OtelClusterReporter(sdk, sbd, history)
    (reporter, reader)
  }

  private def metricValue(reader: InMemoryMetricReader, name: String): Long = {
    import scala.jdk.CollectionConverters.*
    val metrics = reader.collectAllMetrics().asScala
    val metric = metrics
      .find(_.getName == name)
      .getOrElse(fail(s"Metric '$name' not found. Available: ${metrics.map(_.getName).mkString(", ")}"))
    val points = metric.getLongGaugeData.getPoints.asScala
    points.headOption.map(_.getValue).getOrElse(fail(s"No data points for metric '$name'"))
  }

  private def metricDoubleValue(reader: InMemoryMetricReader, name: String): Double = {
    import scala.jdk.CollectionConverters.*
    val metrics = reader.collectAllMetrics().asScala
    val metric = metrics
      .find(_.getName == name)
      .getOrElse(fail(s"Metric '$name' not found. Available: ${metrics.map(_.getName).mkString(", ")}"))
    val points = metric.getDoubleGaugeData.getPoints.asScala
    points.headOption.map(_.getValue).getOrElse(fail(s"No data points for metric '$name'"))
  }

  private def metricLongPointsByAttr(reader: InMemoryMetricReader, name: String): Map[String, Long] = {
    import scala.jdk.CollectionConverters.*
    val metrics = reader.collectAllMetrics().asScala
    val metric = metrics
      .find(_.getName == name)
      .getOrElse(fail(s"Metric '$name' not found"))
    metric.getLongGaugeData.getPoints.asScala.map { pt =>
      val addr = pt.getAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("node_address"))
      addr -> pt.getValue
    }.toMap
  }

  private def metricLongPointsByKey(reader: InMemoryMetricReader, name: String, attrKey: String): Map[String, Long] = {
    import scala.jdk.CollectionConverters.*
    val metrics = reader.collectAllMetrics().asScala
    val metric = metrics
      .find(_.getName == name)
      .getOrElse(fail(s"Metric '$name' not found"))
    metric.getLongGaugeData.getPoints.asScala.map { pt =>
      val key = pt.getAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey(attrKey))
      key -> pt.getValue
    }.toMap
  }

  "OtelClusterReporter" should {

    "start with an empty ClusterStatus" in {
      val reporter = noopReporter
      reporter.currentStatus shouldBe ClusterStatus(Nil, 0, Nil)
    }

    "reflect the latest status after a single update" in {
      val reporter = noopReporter
      val status = ClusterStatus(
        nodes = List(NodeInfo("addr-1", "Up", Set("core"), Nil)),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      reporter.update(status)
      reporter.currentStatus shouldBe status
    }

    "overwrite the previous status on a second update" in {
      val reporter = noopReporter
      val statusV1 = ClusterStatus(List(NodeInfo("addr-1", "Up", Set.empty, Nil)), 0, Nil)
      val statusV2 =
        ClusterStatus(List(NodeInfo("addr-1", "Up", Set.empty, Nil), NodeInfo("addr-2", "Up", Set.empty, Nil)), 0, Nil)
      reporter.update(statusV1)
      reporter.update(statusV2)
      reporter.currentStatus.nodes should have size 2
    }

    "handle an update that removes all nodes (cluster shutdown)" in {
      val reporter = noopReporter
      val initial  = ClusterStatus(List(NodeInfo("addr-1", "Up", Set.empty, Nil)), 0, Nil)
      reporter.update(initial)
      reporter.update(ClusterStatus(Nil, 0, Nil))
      reporter.currentStatus.nodes shouldBe empty
    }

    "be safe to call update concurrently (AtomicReference guarantee)" in {
      val reporter = noopReporter
      val threads = (1 to 10).map { i =>
        new Thread(() => reporter.update(ClusterStatus(List(NodeInfo(s"addr-$i", "Up", Set.empty, Nil)), 0, Nil)))
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
      reporter.currentStatus.nodes should have size 1
    }

    // --- SDK-backed tests that trigger gauge callbacks ---

    "report node count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, Nil),
          NodeInfo("addr-2", "Up", Set.empty, Nil)
        ),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      reporter.update(status)
      metricValue(reader, "cluster.pulse.node.count") shouldBe 2
    }

    "report entity count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 3, List("a", "b", "c")))),
          NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s2", 2, List("d", "e"))))
        ),
        totalEntityCount = 5,
        activeEntities = List("a", "b", "c", "d", "e")
      )
      reporter.update(status)
      metricValue(reader, "cluster.pulse.entity.count") shouldBe 5
    }

    "report unreachable count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, Nil),
          NodeInfo("addr-2", "Unreachable", Set.empty, Nil),
          NodeInfo("addr-3", "Unreachable", Set.empty, Nil)
        ),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      reporter.update(status)
      metricValue(reader, "cluster.pulse.node.unreachable") shouldBe 2
    }

    "report shard count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 1, Nil), ShardInfo("s2", 1, Nil))),
          NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s3", 1, Nil)))
        ),
        totalEntityCount = 3,
        activeEntities = Nil
      )
      reporter.update(status)
      metricValue(reader, "cluster.pulse.shard.count") shouldBe 3
    }

    "report per-region shard count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo(
            "addr-1",
            "Up",
            Set.empty,
            List(
              ShardInfo("s1", 3, Nil, "UserEntity"),
              ShardInfo("s2", 2, Nil, "OrderEntity")
            )
          ),
          NodeInfo(
            "addr-2",
            "Up",
            Set.empty,
            List(
              ShardInfo("s3", 1, Nil, "UserEntity"),
              ShardInfo("s4", 4, Nil, "OrderEntity")
            )
          )
        ),
        totalEntityCount = 10,
        activeEntities = Nil
      )
      reporter.update(status)
      val byRegion = metricLongPointsByKey(reader, "cluster.pulse.shard.region.count", "shard_region")
      byRegion("UserEntity") shouldBe 2
      byRegion("OrderEntity") shouldBe 2
    }

    "not emit shard region metric for shards without regionType" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 3, Nil)))
        ),
        totalEntityCount = 3,
        activeEntities = Nil
      )
      reporter.update(status)
      import scala.jdk.CollectionConverters.*
      val metrics      = reader.collectAllMetrics().asScala
      val regionMetric = metrics.find(_.getName == "cluster.pulse.shard.region.count")
      regionMetric.foreach { m =>
        m.getLongGaugeData.getPoints.asScala shouldBe empty
      }
    }

    "report health score gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, Nil),
          NodeInfo("addr-2", "Up", Set.empty, Nil)
        ),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      reporter.update(status)
      metricValue(reader, "cluster.pulse.health.score") shouldBe 100
    }

    "report shard balance gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 10, Nil))),
          NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s2", 10, Nil)))
        ),
        totalEntityCount = 20,
        activeEntities = Nil
      )
      reporter.update(status)
      metricDoubleValue(reader, "cluster.pulse.shard.balance") shouldBe 0.0
    }

    "report per-node entity count gauge via SDK callback" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 3, Nil), ShardInfo("s2", 2, Nil))),
          NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s3", 7, Nil)))
        ),
        totalEntityCount = 12,
        activeEntities = Nil
      )
      reporter.update(status)
      val perNode = metricLongPointsByAttr(reader, "cluster.pulse.node.entity.count")
      perNode("addr-1") shouldBe 5
      perNode("addr-2") shouldBe 7
    }

    "report per-node status gauge via SDK callback (Up=1, Unreachable=0)" in {
      val (reporter, reader) = sdkReporter()
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, Nil),
          NodeInfo("addr-2", "Unreachable", Set.empty, Nil)
        ),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      reporter.update(status)
      val statusMap = metricLongPointsByAttr(reader, "cluster.pulse.node.status")
      statusMap("addr-1") shouldBe 1L
      statusMap("addr-2") shouldBe 0L
    }

    "report split-brain gauge when SplitBrainDetector is provided" in {
      val sbd                = SplitBrainDetector()
      val (reporter, reader) = sdkReporter(sbd = Some(sbd))
      // Initially not detected
      metricValue(reader, "cluster.pulse.split_brain.detected") shouldBe 0L
      // Trigger split-brain by updating SBD with unreachable majority
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, Nil),
          NodeInfo("addr-2", "Unreachable", Set.empty, Nil),
          NodeInfo("addr-3", "Unreachable", Set.empty, Nil)
        ),
        totalEntityCount = 0,
        activeEntities = Nil
      )
      sbd.update(status)
      metricValue(reader, "cluster.pulse.split_brain.detected") shouldBe 1L
    }

    "report history gauges when ClusterHistory is provided" in {
      val history            = ClusterHistory(maxSize = 5)
      val (reporter, reader) = sdkReporter(history = Some(history))
      // Initially empty
      metricValue(reader, "cluster.pulse.history.size") shouldBe 0L
      // Record some snapshots
      val s1 = ClusterStatus(List(NodeInfo("addr-1", "Up", Set.empty, Nil)), 0, Nil)
      val s2 =
        ClusterStatus(List(NodeInfo("addr-1", "Up", Set.empty, Nil), NodeInfo("addr-2", "Up", Set.empty, Nil)), 0, Nil)
      history.record(s1)
      history.record(s2)
      metricValue(reader, "cluster.pulse.history.size") shouldBe 2L
      metricValue(reader, "cluster.pulse.node.count.delta") shouldBe 1L
    }

    "report all gauges with both SplitBrainDetector and ClusterHistory" in {
      val sbd                = SplitBrainDetector()
      val history            = ClusterHistory(maxSize = 5)
      val (reporter, reader) = sdkReporter(sbd = Some(sbd), history = Some(history))
      val status = ClusterStatus(
        nodes = List(
          NodeInfo("addr-1", "Up", Set.empty, List(ShardInfo("s1", 2, List("e1", "e2")))),
          NodeInfo("addr-2", "Up", Set.empty, List(ShardInfo("s2", 1, List("e3"))))
        ),
        totalEntityCount = 3,
        activeEntities = List("e1", "e2", "e3")
      )
      reporter.update(status)
      history.record(status)
      sbd.update(status)
      metricValue(reader, "cluster.pulse.node.count") shouldBe 2
      metricValue(reader, "cluster.pulse.entity.count") shouldBe 3
      metricValue(reader, "cluster.pulse.split_brain.detected") shouldBe 0L
      metricValue(reader, "cluster.pulse.history.size") shouldBe 1L
    }
  }
}
