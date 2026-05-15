package dev.thenth.clusterpulse

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

class ClusterPulseAutoDiscoverySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers {

  "ClusterStatusTracker" should {
    "accept new type keys via RegisterTypeKey" in {
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      val tracker = spawn(ClusterStatusTracker(sharding, Seq.empty, None, None, None, None))
      
      val typeKey = EntityTypeKey[String]("test-type")
      tracker ! ClusterStatusTracker.RegisterTypeKey(typeKey)
    }
  }

  "StandaloneClusterPulse" should {
    "provide a wrapper for sharding.init" in {
      import org.apache.pekko.cluster.sharding.typed.scaladsl.Entity
      import org.apache.pekko.actor.typed.scaladsl.Behaviors
      
      val sharding = org.mockito.Mockito.mock(classOf[org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding])
      
      val pulse = ClusterPulse.create(system, sharding)
      
      val typeKey = EntityTypeKey[String]("test-type")
      val entity = Entity(typeKey)(_ => Behaviors.empty)
      
      org.mockito.Mockito.when(sharding.init(entity)).thenReturn(null)
      
      pulse.shardingInit(entity)
      
      // Verify that it correctly delegates to sharding
      org.mockito.Mockito.verify(sharding).init(entity)
    }
  }
}
