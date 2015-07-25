organization := Common.organization

name := "s2graph"

version := Common.version

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
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.hbase" % "hbase-server" % Common.hbaseVersion excludeAll(ExclusionRule(organization = "org.slf4j"), ExclusionRule(organization = "com.google.protobuf")),
  "org.apache.hadoop" % "hadoop-common" % Common.hadoopVersion excludeAll ExclusionRule(organization = "org.slf4j"),
  "nl.grons" %% "metrics-scala" % "3.4.0"
   )

   
