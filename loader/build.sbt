import sbtassembly.Plugin.AssemblyKeys._

name := "s2loader"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "org.apache.httpcomponents" % "fluent-hc" % "4.2.5",
  "org.specs2" %% "specs2-core" % "2.4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.14-hadoop2",
  "org.apache.hadoop" % "hadoop-distcp" % Common.hadoopVersion
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
