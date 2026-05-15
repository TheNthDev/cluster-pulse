package dev.thenth.clusterpulse

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Source

import dev.thenth.clusterpulse.model.ClusterStatus

trait ClusterStatusService {

  /** Returns a single snapshot of the current cluster status. */
  def getStatus: Future[ClusterStatus]

  /** Returns a live stream of ClusterStatus snapshots at a fixed interval. */
  def statusStream(): Source[ClusterStatus, ?]
}
