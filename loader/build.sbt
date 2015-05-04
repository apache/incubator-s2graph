import sbtassembly.Plugin.AssemblyKeys._

name := "s2graph-loader"

version := Common.version

scalaVersion := Common.scalaVersion

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "edu.berkeley.cs.amplab" %% "spark-indexedrdd" % "0.1-SNAPSHOT",
  "org.elasticsearch" % "elasticsearch" % "1.3.2",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0" % "provided",
  "org.apache.httpcomponents" % "fluent-hc" % "4.2.5",
  "org.specs2" %% "specs2-core" % "2.4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Common.resolvers

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
