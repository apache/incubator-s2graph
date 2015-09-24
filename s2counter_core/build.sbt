name := "s2counter-core"

scalacOptions ++= Seq("-feature", "-deprecation", "-language:existentials")

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.play" %% "play-json" % Common.playVersion,
  "com.typesafe.play" %% "play-ws" % Common.playVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion,
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion,
  "org.apache.hadoop" % "hadoop-common" % Common.hadoopVersion,
  "redis.clients" % "jedis" % "2.6.0",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
  "mysql" % "mysql-connector-java" % "5.1.28",
  "org.scalikejdbc" %% "scalikejdbc" % "2.1.+",
  "org.specs2" %% "specs2-core" % "3.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalaj" %% "scalaj-http" % "1.1.4"
).map { moduleId =>
  moduleId.exclude("org.slf4j", "slf4j-log4j12")
}
