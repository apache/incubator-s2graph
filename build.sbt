organization := Common.organization

name := "s2graph"

version := Common.version

scalaVersion := Common.scalaVersion

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers ++= Common.resolvers

lazy val root = project.in(file(".")).enablePlugins(PlayScala).dependsOn(s2core)

lazy val s2core = project

lazy val spark = project

lazy val loader = project.dependsOn(s2core, spark)

libraryDependencies ++= Seq(
  anorm,
  cache,
  ws,
  filters,
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-server" % Common.hbaseVersion excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.google.protobuf")),
  "org.apache.hadoop" % "hadoop-common" % Common.hadoopVersion excludeAll ExclusionRule(organization = "org.slf4j")
)
