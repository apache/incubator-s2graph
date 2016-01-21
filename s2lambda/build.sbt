import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._

name := "s2lambda"

scalacOptions ++= Seq("-deprecation")

val sparkVersion = Common.s2lambdaSparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("org.jboss.netty", "netty"),
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.2.10",
  "com.github.mdr" % "ascii-graphs_2.10" % "0.0.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

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
