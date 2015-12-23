import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._

name := "s2ml"

scalacOptions ++= Seq("-deprecation")

val sparkVersion = "1.5.1"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("org.jboss.netty", "netty"),
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.typesafe.play" %% "play-ws" % Common.playVersion,
  "org.json4s" %% "json4s-native" % "3.2.10",
  "com.github.mdr" % "ascii-graphs_2.10" % "0.0.3",
  "com.github.nscala-time" %% "nscala-time" % "2.4.0",
  "com.thesamet" %% "kdtree" % "1.0.4",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.6")

assemblySettings

mergeStrategy in assembly := {
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "guava-16.0.1.jar"}
}

test in assembly := {}
