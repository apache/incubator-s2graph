//import AssemblyKeys._
name := "s2graph-spark"

version := Common.version

scalaVersion := Common.scalaVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0",
  "com.typesafe.play" %% "play-json" % "2.3.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Common.resolvers
