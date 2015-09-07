name := "s2spark"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "com.typesafe.play" %% "play-json" % Common.playVersion,
  "org.specs2" %% "specs2-core" % "3.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
