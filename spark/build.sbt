//import AssemblyKeys._
name := "s2spark"

organization := Common.organization

version := Common.version

scalaVersion := Common.scalaVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "com.typesafe.play" %% "play-json" % "2.3.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Common.resolvers

