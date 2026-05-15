package dev.thenth.clusterpulse

import scala.concurrent.Future
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import dev.thenth.clusterpulse.ClusterStatusTracker.*
import dev.thenth.clusterpulse.model.ClusterStatus

/** Production implementation of [[ClusterStatusService]] that queries the [[ClusterStatusTracker]] actor for the
  * current cluster status.
  *
  * `getStatus` performs a single ask; `statusStream` emits snapshots on a configurable tick interval read from
  * [[ClusterPulseSettings]].
  *
  * @param tracker
  *   reference to the running [[ClusterStatusTracker]] actor
  * @param system
  *   the actor system (used for ask-pattern scheduling and config)
  */
class LiveClusterStatusService(
  tracker: ActorRef[Command]
)(implicit system: ActorSystem[?])
    extends ClusterStatusService {

  private val settings                  = ClusterPulseSettings(system.settings.config)
  private implicit val timeout: Timeout = settings.askTimeout

  override def getStatus: Future[ClusterStatus] =
    tracker
      .ask[Response](GetStatus.apply)
      .collect { case ClusterStatusResponse(status) =>
        status
      }(using system.executionContext)

  override def statusStream(): Source[ClusterStatus, ?] =
    Source
      .tick(0.seconds, settings.streamInterval, ())
      .mapAsync(1)(_ => tracker.ask[Response](GetStatus.apply))
      .collect { case ClusterStatusResponse(status) => status }
}
