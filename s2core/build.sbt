import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import sbtassembly.Plugin.AssemblyKeys._


name := """s2core"""

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
  "org.hbase" % "asynchbase" % "1.7.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
   )
   
