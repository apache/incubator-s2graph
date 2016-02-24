
name := "s2rest_play"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  ws,
  filters,
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3"
)

enablePlugins(JavaServerAppPackaging)
