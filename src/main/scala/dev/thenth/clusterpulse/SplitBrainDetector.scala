package dev.thenth.clusterpulse

import java.util.concurrent.atomic.AtomicReference

import dev.thenth.clusterpulse.model.ClusterStatus

/** Heuristic split-brain detector that tracks cluster state across snapshots and flags potential network partitions.
  *
  * Detection heuristics:
  *   1. **Unreachable majority**: more than half the nodes are unreachable.
  *   2. **Rapid membership change**: a large fraction of nodes appeared or disappeared between consecutive snapshots
  *      (configurable threshold).
  *   3. **Leader gap**: the cluster has nodes but none report a "Up" status (all are Joining, WeaklyUp, Leaving,
  *      Exiting, Down, or Unreachable).
  *
  * This is a **complement** to a proper Split Brain Resolver (SBR) — it detects symptoms and exposes a gauge, but does
  * not take corrective action.
  *
  * @param membershipChangeThreshold
  *   fraction (0.0–1.0) of nodes that must change between snapshots to trigger the rapid-change heuristic.
  */
class SplitBrainDetector(membershipChangeThreshold: Double = 0.5) {

  private case class State(
    detected: Boolean,
    reason: Option[String],
    previous: Option[ClusterStatus]
  )

  private val state = new AtomicReference[State](State(detected = false, reason = None, previous = None))

  /** Update the detector with a new cluster snapshot. Returns true if split-brain is suspected. */
  def update(status: ClusterStatus): Boolean = {
    val newState = evaluate(status, state.get().previous)
    state.set(newState.copy(previous = Some(status)))
    newState.detected
  }

  /** Whether a split-brain is currently suspected. */
  def isDetected: Boolean = state.get().detected

  /** Human-readable reason for the current detection, if any. */
  def reason: Option[String] = state.get().reason

  private def evaluate(current: ClusterStatus, previous: Option[ClusterStatus]): State =
    if (current.nodes.isEmpty) {
      State(detected = false, reason = None, previous = previous)
    } else {
      val unreachable = current.unreachableCount
      if (unreachable > 0 && unreachable >= (current.nodes.size / 2.0)) {
        // Heuristic 1: unreachable majority
        State(
          detected = true,
          reason = Some(s"Unreachable majority: $unreachable of ${current.nodes.size} nodes unreachable"),
          previous = previous
        )
      } else {
        checkRapidChange(current, previous).getOrElse {
          // Heuristic 3: no healthy leader — all nodes are in non-Up states
          val upNodes = current.nodes.count(n => n.status == "Up")
          if (upNodes == 0) {
            State(
              detected = true,
              reason = Some(s"No Up nodes: all ${current.nodes.size} nodes in non-Up state"),
              previous = previous
            )
          } else {
            State(detected = false, reason = None, previous = previous)
          }
        }
      }
    }

  private def checkRapidChange(current: ClusterStatus, previous: Option[ClusterStatus]): Option[State] =
    previous.flatMap { prev =>
      if (prev.nodes.nonEmpty) {
        val prevAddrs = prev.nodes.map(_.address).toSet
        val currAddrs = current.nodes.map(_.address).toSet
        val changed   = (prevAddrs.diff(currAddrs) ++ currAddrs.diff(prevAddrs)).size
        val total     = math.max(prevAddrs.size, currAddrs.size)
        if (total > 0 && changed.toDouble / total >= membershipChangeThreshold) {
          Some(
            State(
              detected = true,
              reason = Some(s"Rapid membership change: $changed of $total nodes changed"),
              previous = previous
            )
          )
        } else None
      } else None
    }
}
