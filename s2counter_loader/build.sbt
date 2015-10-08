import sbtassembly.Plugin.AssemblyKeys._

name := "s2counter-loader"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "com.typesafe.play" %% "play-ws" % Common.playVersion,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
).map { id =>
  id.excludeAll(ExclusionRule(organization = "javax.servlet"), ExclusionRule(organization = "org.mortbay.jetty"), ExclusionRule(organization = "com.google.guava"))
}

// force specific library version
libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "16.0.1"
)

fork := true

transitiveClassifiers ++= Seq()

assemblySettings

mergeStrategy in assembly := {
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}
