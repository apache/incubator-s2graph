name := "s2rest_play"

version := "0.12.1-SNAPSHOT"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  ws,
  filters,
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3"
)

enablePlugins(JavaServerAppPackaging)

