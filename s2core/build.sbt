name := """s2core"""

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.play" %% "play-json" % Common.playVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-server" % Common.hbaseVersion excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.google.protobuf")),
  "org.apache.hadoop" % "hadoop-common" % Common.hadoopVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "commons-pool" % "commons-pool" % "1.6",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalikejdbc" %% "scalikejdbc" % "2.1.+",
  "mysql" % "mysql-connector-java" % "5.1.28",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0" excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.sun.jdmk"), ExclusionRule(organization = "com.sun.jmx"), ExclusionRule(organization = "javax.jms")),
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.4"
)

libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    // if scala 2.11+ is used, add dependency on scala-xml mqqodule
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value ++ Seq(
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
      )
    case _ =>
      libraryDependencies.value
  }
}
