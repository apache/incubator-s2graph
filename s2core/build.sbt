import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import sbtassembly.Plugin.AssemblyKeys._

name := """s2core"""

organization := Common.organization

version := Common.version

scalaVersion := Common.scalaVersion

scalacOptions ++= Seq("-deprecation")

resolvers ++= Common.resolvers

libraryDependencies ++= Seq(
  anorm,
  cache,
  ws,
  filters,
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-server" % Common.hbaseVersion excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.google.protobuf")),
  "org.apache.hadoop" % "hadoop-common" % Common.hadoopVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "commons-pool" % "commons-pool" % "1.6",
  "org.hbase" % "asynchbase" % "1.8.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalikejdbc" %% "scalikejdbc"        % "2.1.+",
  "mysql" % "mysql-connector-java" % "5.1.28",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0" excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.sun.jdmk"), ExclusionRule(organization = "com.sun.jmx"), ExclusionRule(organization = "javax.jms"))
   )

parallelExecution in Test := false

publishTo := {
  val dk = "http://maven.daumcorp.com/content/repositories/"
  if (isSnapshot.value)
    Some("snapshots" at dk + "dk-aa-snapshots")
  else
    Some("releases"  at dk + "dk-aa-release")
}
