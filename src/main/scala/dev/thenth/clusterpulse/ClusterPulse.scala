package dev.thenth.clusterpulse

import scala.concurrent.Future

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Extension, ExtensionId}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import org.apache.pekko.stream.scaladsl.Source

import io.opentelemetry.api.trace.Tracer

import dev.thenth.clusterpulse.ClusterStatusTracker.*
import dev.thenth.clusterpulse.model.ClusterStatus

/** Pekko Extension for cluster-pulse.
  *
  * Idiomatic usage via the Extension mechanism:
  * {{{
  * // Access the singleton extension (auto-creates tracker with defaults)
  * val pulse = ClusterPulse(system)
  * val status: Future[ClusterStatus] = pulse.status
  *
  * // Auto-discover entity types
  * val ref = pulse.shardingInit(Entity(MyEntity.TypeKey)(createBehavior))
  * }}}
  *
  * Standalone (non-extension) usage for custom configurations:
  * {{{
  * val pulse = ClusterPulse.create(system, sharding, Seq(MyEntity.TypeKey))
  * }}}
  */
class ClusterPulse(system: ActorSystem[?]) extends Extension {

  private given ActorSystem[?]  = system
  private given ClusterSharding = ClusterSharding(system)

  private val settings = ClusterPulseSettings(system.settings.config)

  private val _history: Option[ClusterHistory] =
    Some(new ClusterHistory(settings.historyBufferSize))

  private val _splitBrainDetector: Option[SplitBrainDetector] =
    Some(new SplitBrainDetector(settings.splitBrainMembershipThreshold))

  private val _tracker: ActorRef[Command] = {
    import org.apache.pekko.actor.typed.scaladsl.adapter.*
    system.toClassic.toTyped
      .systemActorOf(
        ClusterStatusTracker(
          summon[ClusterSharding],
          Seq.empty,
          None,
          _history,
          _splitBrainDetector,
          None
        ),
        "ClusterPulseTracker"
      )
  }

  private val _service: ClusterStatusService =
    new LiveClusterStatusService(_tracker)

  /** The underlying tracker actor. */
  def tracker: ActorRef[Command] = _tracker

  /** The cluster status service. */
  def service: ClusterStatusService = _service

  /** The rolling history buffer (always available on the extension). */
  def history: ClusterHistory = _history.get

  /** The split-brain detector (always available on the extension). */
  def splitBrainDetector: SplitBrainDetector = _splitBrainDetector.get

  /** One-shot current cluster status. */
  def status: Future[ClusterStatus] = _service.getStatus

  /** Live stream of cluster status snapshots. */
  def statusStream(): Source[ClusterStatus, ?] = _service.statusStream()

  /** Register an entity type key for tracking.
    */
  def registerEntity(typeKey: EntityTypeKey[?]): Unit =
    _tracker ! RegisterTypeKey(typeKey)

  /** Wrapper for `sharding.init` that automatically registers the entity type key.
    */
  def shardingInit[M, E](entity: Entity[M, E]): ActorRef[E] = {
    val ref = summon[ClusterSharding].init(entity)
    registerEntity(entity.typeKey)
    ref
  }
}

/** ExtensionId for ClusterPulse — provides singleton access via `ClusterPulse(system)`.
  *
  * Also provides `create` factory methods for standalone (non-extension) instances with custom configurations.
  */
object ClusterPulse extends ExtensionId[ClusterPulse] {

  override def createExtension(system: ActorSystem[?]): ClusterPulse =
    new ClusterPulse(system)

  // ---------------------------------------------------------------------------
  // Standalone factory methods (non-extension, for custom configurations)
  // ---------------------------------------------------------------------------

  /** Create a standalone ClusterPulse instance without initial keys (rely on auto-discovery).
    */
  def create(
    system: ActorSystem[?],
    sharding: ClusterSharding,
    reporter: Option[OtelClusterReporter] = None,
    history: Option[ClusterHistory] = None,
    splitBrainDetector: Option[SplitBrainDetector] = None,
    tracer: Option[Tracer] = None
  ): StandaloneClusterPulse = create(system, sharding, Seq.empty, reporter, history, splitBrainDetector, tracer)

  /** Create a standalone ClusterPulse instance with a single entity type key.
    */
  def create[T](
    system: ActorSystem[?],
    sharding: ClusterSharding,
    typeKey: EntityTypeKey[T],
    reporter: Option[OtelClusterReporter],
    history: Option[ClusterHistory],
    splitBrainDetector: Option[SplitBrainDetector],
    tracer: Option[Tracer]
  ): StandaloneClusterPulse = create(system, sharding, Seq(typeKey), reporter, history, splitBrainDetector, tracer)

  /** Create a standalone ClusterPulse instance with multiple entity type keys.
    */
  def create(
    system: ActorSystem[?],
    sharding: ClusterSharding,
    typeKeys: Seq[EntityTypeKey[?]],
    reporter: Option[OtelClusterReporter],
    history: Option[ClusterHistory],
    splitBrainDetector: Option[SplitBrainDetector],
    tracer: Option[Tracer]
  ): StandaloneClusterPulse = {
    import org.apache.pekko.actor.typed.scaladsl.adapter.*
    val tracker = system.toClassic.toTyped
      .systemActorOf(
        ClusterStatusTracker(sharding, typeKeys, reporter, history, splitBrainDetector, tracer),
        s"ClusterPulseTracker-${java.util.UUID.randomUUID().toString.take(8)}"
      )
    val service = new LiveClusterStatusService(tracker)(using system)
    new StandaloneClusterPulse(tracker, service, history, splitBrainDetector)(using system, sharding)
  }
}

/** A standalone (non-extension) ClusterPulse instance for custom configurations. Use `ClusterPulse.create(...)` to
  * construct.
  */
class StandaloneClusterPulse(
  val tracker: ActorRef[Command],
  val service: ClusterStatusService,
  val history: Option[ClusterHistory],
  val splitBrainDetector: Option[SplitBrainDetector]
)(using system: ActorSystem[?], sharding: ClusterSharding) {

  /** One-shot current cluster status. */
  def status: Future[ClusterStatus] = service.getStatus

  /** Live stream of cluster status snapshots. */
  def statusStream(): Source[ClusterStatus, ?] = service.statusStream()

  /** Register an entity type key for tracking.
    */
  def registerEntity(typeKey: EntityTypeKey[?]): Unit =
    tracker ! RegisterTypeKey(typeKey)

  /** Wrapper for `sharding.init` that automatically registers the entity type key.
    */
  def shardingInit[M, E](entity: Entity[M, E]): ActorRef[E] = {
    val ref = sharding.init(entity)
    registerEntity(entity.typeKey)
    ref
  }
}
