import sbtassembly.Plugin.AssemblyKeys._

name := "s2ml"

scalacOptions ++= Seq("-deprecation")

val sparkVersion = "1.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.json4s" %% "json4s-native" % "3.3.0",
  "com.github.mdr" %% "ascii-graphs" % "0.0.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

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
