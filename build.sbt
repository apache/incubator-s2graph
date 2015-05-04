organization := "com.daumkakao.s2graph"

name := "s2graph"

version := "0.4.0-SNAPSHOT"

scalaVersion := Common.scalaVersion

resolvers ++= Common.resolvers

lazy val root = project.in(file(".")).enablePlugins(PlayScala).dependsOn(s2core)

//lazy val root = project.in(file(".")).aggregate(s2rest)

lazy val s2core = project

//lazy val s2rest = project.enablePlugins(PlayScala).dependsOn(s2core)

lazy val spark = project

lazy val loader = project.dependsOn(s2core, spark)

libraryDependencies ++= Seq(
  anorm,
  cache,
  ws,
  filters,
  "org.apache.hbase" % "hbase-client" % "0.98.6-cdh5.3.0" excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-common" % "0.98.6-cdh5.3.0" excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-server" % "0.98.6-cdh5.3.0" excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.google.protobuf")),
  "org.apache.hadoop" % "hadoop-common" % "2.5.0-cdh5.3.0" excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0" excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.sun.jdmk"), ExclusionRule(organization = "com.sun.jmx"), ExclusionRule(organization = "javax.jms")),
  "mysql" % "mysql-connector-java" % "5.1.28",
  "redis.clients" % "jedis" % "2.4.2",
  "commons-pool" % "commons-pool" % "1.6",
  "org.scalikejdbc" %% "scalikejdbc"        % "2.1.+",
  "com.twitter" %% "util-collection" % "6.12.1",
  "com.wordnik" %% "swagger-play2" % "1.3.10",
  "com.newrelic.agent.java" % "newrelic-api" % "3.14.0", 
  "org.hbase" % "asynchbase" % "1.7.0-SNAPSHOT",
  "nl.grons" %% "metrics-scala" % "3.4.0"
   )

   
unmanagedResourceDirectories in Compile += baseDirectory.value / "res"
