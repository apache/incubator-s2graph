name := "s2rest_play"

version := "0.12.1-SNAPSHOT"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  ws,
  filters,
  "xalan" % "serializer" % "2.7.2", // Download in Intelli J(Download Source/Document)
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3",
  "org.json4s" %% "json4s-native" % "3.2.11" % Test
)
