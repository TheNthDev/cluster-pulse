package dev.thenth.clusterpulse

/**
 * Marker trait for messages that need cluster serialization.
 * Consumers should configure their serialization binding to map
 * this trait to their preferred serializer (e.g., Jackson CBOR).
 *
 * Example `application.conf`:
 * {{{
 * pekko.actor.serialization-bindings {
 *   "dev.thenth.clusterpulse.ClusterPulseSerializable" = jackson-cbor
 * }
 * }}}
 */
trait ClusterPulseSerializable
