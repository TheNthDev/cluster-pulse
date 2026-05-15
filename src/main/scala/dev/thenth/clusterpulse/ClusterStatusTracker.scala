package dev.thenth.clusterpulse

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.GetShardRegionState
import org.apache.pekko.cluster.sharding.ShardRegion.CurrentShardRegionState
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.cluster.ClusterEvent.*
import org.apache.pekko.util.Timeout

import io.opentelemetry.api.trace.{Span, StatusCode, Tracer}

import dev.thenth.clusterpulse.model.{ClusterStatus, NodeInfo, ShardInfo}

object ClusterStatusTracker {

  val ClusterStatusTrackerKey: ServiceKey[Command] = ServiceKey[Command]("ClusterStatusTracker")

  sealed trait Command                                  extends ClusterPulseSerializable
  sealed trait Response                                 extends ClusterPulseSerializable
  private case object Tick                              extends Command
  private case object ReportMetrics                     extends Command
  case class RegisterTypeKey(typeKey: EntityTypeKey[?]) extends Command
  case class GetStatus(replyTo: ActorRef[Response])     extends Command

  case class GetLocalState(replyTo: ActorRef[LocalStateResponse]) extends Command
  case class LocalStateResponse(
    address: String,
    state: CurrentShardRegionState,
    statesByRegion: Map[String, CurrentShardRegionState] = Map.empty
  ) extends Response

  private case class ReportClusterStats(
    replyTo: Option[ActorRef[Response]],
    members: List[org.apache.pekko.cluster.Member],
    allLocalStates: List[LocalStateResponse],
    span: Option[Span]
  ) extends Command
      with ClusterPulseSerializable

  case class ClusterStatusResponse(status: ClusterStatus) extends Response

  /** Create a tracker for a single entity type key (backward-compatible). */
  def apply[T](
    sharding: ClusterSharding,
    typeKey: EntityTypeKey[T],
    reporter: Option[OtelClusterReporter] = None,
    history: Option[ClusterHistory] = None,
    splitBrainDetector: Option[SplitBrainDetector] = None,
    tracer: Option[Tracer] = None
  ): Behavior[Command] = apply(sharding, Seq(typeKey), reporter, history, splitBrainDetector, tracer)

  /** Create a tracker for multiple entity type keys. */
  def apply(
    sharding: ClusterSharding,
    typeKeys: Seq[EntityTypeKey[?]],
    reporter: Option[OtelClusterReporter],
    history: Option[ClusterHistory],
    splitBrainDetector: Option[SplitBrainDetector],
    tracer: Option[Tracer]
  ): Behavior[Command] = Behaviors.setup { context =>
    val settings                  = ClusterPulseSettings(context.system.settings.config)
    implicit val timeout: Timeout = settings.askTimeout
    val includeEntityIds          = settings.includeEntityIds

    def active(currentTypeKeys: Set[EntityTypeKey[?]]): Behavior[Command] = Behaviors.withTimers { timers =>
      context.system.receptionist ! Receptionist.Register(ClusterStatusTrackerKey, context.self)

      import context.system
      import org.apache.pekko.actor.typed.scaladsl.adapter._
      val cluster = Cluster(context.system)

      val clusterEventResponseAdapter: ActorRef[ClusterDomainEvent] = context
        .messageAdapter(_ => Tick)

      cluster.subscriptions ! Subscribe(clusterEventResponseAdapter, classOf[ClusterDomainEvent])

      reporter.foreach { _ =>
        timers.startTimerWithFixedDelay(ReportMetrics, settings.reportInterval)
      }

      Behaviors.receiveMessage {
        case Tick => Behaviors.same

        case RegisterTypeKey(typeKey) =>
          active(currentTypeKeys + typeKey)

        case GetStatus(replyTo) =>
          gatherClusterStats(context, cluster, currentTypeKeys, Some(replyTo), timeout, tracer)
          Behaviors.same

        case ReportMetrics =>
          gatherClusterStats(context, cluster, currentTypeKeys, None, timeout, tracer)
          Behaviors.same

        case GetLocalState(replyTo) =>
          implicit val ec: ExecutionContextExecutor = context.system.executionContext
          val address                               = cluster.selfMember.address.toString

          val allStatesFuture = Future.sequence(currentTypeKeys.toSeq.map { typeKey =>
            sharding.shardState
              .ask[CurrentShardRegionState](
                GetShardRegionState(typeKey, _)
              )(timeout, context.system.scheduler)
              .map(s => typeKey.name -> s)
          })

          allStatesFuture.onComplete {
            case Success(pairs) =>
              val byRegion = pairs.toMap
              val merged   = CurrentShardRegionState(pairs.flatMap(_._2.shards).toSet)
              replyTo ! LocalStateResponse(address, merged, byRegion)
            case Failure(_) =>
              replyTo ! LocalStateResponse(address, CurrentShardRegionState(Set.empty))
          }
          Behaviors.same

        case ReportClusterStats(replyTo, members, allLocalStates, span) =>
          val nodes = members.map { m =>
            val nodeAddress    = m.address.toString
            val localStateResp = allLocalStates.find(_.address == nodeAddress)
            val shards = localStateResp
              .map { resp =>
                // Build a lookup from shardId to region type name
                val shardToRegion: Map[String, String] = resp.statesByRegion.flatMap { case (regionName, regionState) =>
                  regionState.shards.map(s => s.shardId -> regionName)
                }
                resp.state.shards.map { s =>
                  val ids = if (includeEntityIds) s.entityIds.toList else Nil
                  ShardInfo(s.shardId, s.entityIds.size, ids, shardToRegion.getOrElse(s.shardId, ""))
                }.toList
              }
              .toList
              .flatten
            val status =
              if (cluster.state.unreachable.contains(m)) "Unreachable" else m.status.toString
            NodeInfo(nodeAddress, status, m.roles, shards)
          }
          val totalCount = nodes.flatMap(_.shards.map(_.entityCount)).sum
          val allActiveEntities =
            if (includeEntityIds) allLocalStates.flatMap(_.state.shards.toSeq.flatMap(_.entityIds))
            else Nil
          val clusterStatus = ClusterStatus(nodes, totalCount, allActiveEntities)

          replyTo.foreach(_ ! ClusterStatusResponse(clusterStatus))
          reporter.foreach(_.update(clusterStatus))
          history.foreach(_.record(clusterStatus))
          splitBrainDetector.foreach(_.update(clusterStatus))

          span.foreach { s =>
            s.setAttribute("cluster.node.count", nodes.size.toLong)
            s.setAttribute("cluster.entity.count", totalCount.toLong)
            s.setStatus(StatusCode.OK)
            s.end()
          }

          Behaviors.same
      }
    }

    active(typeKeys.toSet)
  }

  private def gatherClusterStats(
    context: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
    cluster: Cluster,
    currentTypeKeys: Set[EntityTypeKey[?]],
    replyTo: Option[ActorRef[Response]],
    timeout: Timeout,
    tracer: Option[Tracer]
  ): Unit = {
    implicit val ec: ExecutionContextExecutor = context.system.executionContext
    implicit val t: Timeout                   = timeout

    val span = tracer.map { t =>
      t.spanBuilder("cluster-pulse.gather-stats")
        .setAttribute("cluster.self.address", cluster.selfMember.address.toString)
        .startSpan()
    }

    val trackersFuture: Future[Receptionist.Listing] = context.system.receptionist
      .ask(Receptionist.Find(ClusterStatusTrackerKey))(timeout, context.system.scheduler)

    val resultFuture = for {
      listing <- trackersFuture
      trackers = listing.serviceInstances(ClusterStatusTrackerKey)
      responses <- Future.sequence(trackers.map { tracker =>
        tracker.ask[LocalStateResponse](GetLocalState(_))(timeout, context.system.scheduler)
      })
    } yield responses.toList

    context.pipeToSelf(resultFuture) {
      case Success(responses) =>
        ReportClusterStats(replyTo, cluster.state.members.toList, responses, span)
      case Failure(e) =>
        context.log.error("Failed to gather cluster stats", e)
        span.foreach { s =>
          s.setStatus(StatusCode.ERROR, e.getMessage)
          s.recordException(e)
          s.end()
        }
        ReportClusterStats(replyTo, cluster.state.members.toList, Nil, None)
    }
  }
}
