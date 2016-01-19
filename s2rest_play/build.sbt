
import com.typesafe.sbt.packager.docker._

name := "s2rest_play"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  ws,
  filters,
  "xalan" % "serializer" % "2.7.2", // Download in Intelli J(Download Source/Document)
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3"
)

enablePlugins(JavaServerAppPackaging)

enablePlugins(DockerPlugin)

dockerBaseImage := "isuper/java-oracle"

dockerExposedPorts := Seq(9000)
