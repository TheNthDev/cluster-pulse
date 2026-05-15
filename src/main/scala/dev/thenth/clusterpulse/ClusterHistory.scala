package dev.thenth.clusterpulse

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.Queue

import dev.thenth.clusterpulse.model.{ClusterEvent, ClusterStatus}

/**
 * Thread-safe rolling history buffer that keeps the last N cluster status snapshots
 * and emits structured change events by diffing consecutive snapshots.
 *
 * @param maxSize maximum number of snapshots to retain (defaults to config value or 60)
 */
class ClusterHistory(val maxSize: Int = 60) {

  private case class State(
    snapshots: Queue[ClusterStatus],
    events: Queue[ClusterEvent]
  )

  private val state = new AtomicReference[State](State(Queue.empty, Queue.empty))

  /** Maximum number of events to retain (5x snapshot buffer). */
  private val maxEvents: Int = maxSize * 5

  /**
   * Record a new snapshot. Automatically diffs against the previous snapshot
   * and appends any resulting events to the event log.
   */
  def record(status: ClusterStatus): List[ClusterEvent] = {
    var emitted: List[ClusterEvent] = Nil
    state.getAndUpdate { s =>
      val now       = System.currentTimeMillis()
      val newEvents = s.snapshots.lastOption.map(prev => ClusterEvent.diff(prev, status, now)).getOrElse(Nil)
      emitted = newEvents

      val updatedSnapshots = {
        val q = s.snapshots.enqueue(status)
        if (q.size > maxSize) q.tail else q
      }
      val updatedEvents = {
        var q = s.events
        newEvents.foreach(e => q = q.enqueue(e))
        while (q.size > maxEvents) q = q.tail
        q
      }
      State(updatedSnapshots, updatedEvents)
    }
    emitted
  }

  /** Returns all retained snapshots, oldest first. */
  def snapshots: List[ClusterStatus] = state.get().snapshots.toList

  /** Returns the most recent snapshot, if any. */
  def latest: Option[ClusterStatus] = state.get().snapshots.lastOption

  /** Returns all retained events, oldest first. */
  def events: List[ClusterEvent] = state.get().events.toList

  /** Returns the number of retained snapshots. */
  def size: Int = state.get().snapshots.size

  /**
   * Node count delta: difference between the latest and oldest snapshot's node count.
   * Returns 0 if fewer than 2 snapshots are available.
   */
  def nodeCountDelta: Int = {
    val snaps = state.get().snapshots
    if (snaps.size < 2) 0
    else snaps.last.nodes.size - snaps.head.nodes.size
  }
}
