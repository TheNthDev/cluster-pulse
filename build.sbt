organization := "dev.thenth"
name         := "cluster-pulse"
version      := "1.0.0"
scalaVersion := "3.8.1"

homepage := Some(url("https://github.com/TheNthDev/cluster-pulse"))
licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
scmInfo := Some(ScmInfo(
  url("https://github.com/TheNthDev/cluster-pulse"),
  "scm:git@github.com:TheNthDev/cluster-pulse.git"
))
developers := List(
  Developer("TheNthDev", "cluster-pulse contributors", "", url("https://github.com/TheNthDev"))
)

publishMavenStyle      := true
pomIncludeRepository   := { _ => false }
description            := "Lightweight cluster monitoring for Apache Pekko Cluster Sharding"
publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

val pekkoVersion     = "1.6.0"
val pekkoHttpVersion = "1.3.0"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed"             % pekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-sharding-typed"  % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream-typed"            % pekkoVersion,
  "org.apache.pekko" %% "pekko-serialization-jackson"   % pekkoVersion,
  "org.apache.pekko" %% "pekko-http"                    % pekkoHttpVersion,
  "io.spray"         %% "spray-json"                    % "1.3.6",
  "io.opentelemetry"  % "opentelemetry-api"             % "1.62.0",
  "io.opentelemetry"  % "opentelemetry-sdk"             % "1.62.0"      % Test,
  "io.opentelemetry"  % "opentelemetry-sdk-testing"     % "1.62.0"      % Test,
  "org.scalatest"     %% "scalatest"                    % "3.2.20"      % Test,
  "org.scalatestplus" %% "mockito-5-12"                 % "3.2.19.0"    % Test,
  "org.apache.pekko"  %% "pekko-actor-testkit-typed"    % pekkoVersion  % Test,
  "org.apache.pekko"  %% "pekko-http-testkit"           % pekkoHttpVersion % Test
)

coverageMinimumStmtTotal      := 80
coverageMinimumBranchTotal    := 75
coverageFailOnMinimum         := true
coverageHighlighting          := true
