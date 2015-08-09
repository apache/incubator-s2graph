organization := Common.organization

name := "s2graph"

version := Common.version

scalaVersion := Common.scalaVersion

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

javaOptions ++= collection.JavaConversions.propertiesAsScalaMap(System.getProperties).map{ case (key, value) => "-D" + key + "=" + value }.toSeq

resolvers ++= Common.resolvers

lazy val root = project.in(file(".")).enablePlugins(PlayScala).dependsOn(s2core)

lazy val s2core = project

lazy val spark = project

lazy val loader = project.dependsOn(s2core, spark)

libraryDependencies ++= Seq(ws, filters)
