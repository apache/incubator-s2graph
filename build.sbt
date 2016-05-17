/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

name := "s2graph"

lazy val commonSettings = Seq(
  organization := "com.kakao.s2graph",
  scalaVersion := "2.11.7",
  version := "0.12.1-SNAPSHOT",
  scalacOptions := Seq("-language:postfixOps", "-unchecked", "-deprecation", "-feature", "-Xlint"),
  javaOptions ++= collection.JavaConversions.propertiesAsScalaMap(System.getProperties).map { case (key, value) => "-D" + key + "=" + value }.toSeq,
  testOptions in Test += Tests.Argument("-oDF"),
  parallelExecution in Test := false,
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
  )
)

Revolver.settings

lazy val s2rest_play = project.enablePlugins(PlayScala)
  .dependsOn(s2core, s2counter_core)
  .settings(commonSettings: _*)
  .settings(testOptions in Test += Tests.Argument("sequential"))

lazy val s2rest_netty = project
  .dependsOn(s2core)
  .settings(commonSettings: _*)

lazy val s2core = project.settings(commonSettings: _*)

lazy val spark = project.settings(commonSettings: _*)

lazy val loader = project.dependsOn(s2core, spark)
  .settings(commonSettings: _*)

lazy val s2counter_core = project.dependsOn(s2core)
  .settings(commonSettings: _*)

lazy val s2counter_loader = project.dependsOn(s2counter_core, spark)
  .settings(commonSettings: _*)

lazy val root = (project in file("."))
  .aggregate(s2core, s2rest_play)
  .dependsOn(s2rest_play, s2rest_netty, loader, s2counter_loader) // this enables packaging on the root project
  .settings(commonSettings: _*)

lazy val runRatTask = taskKey[Unit]("Runs Apache rat on S2Graph")

runRatTask := {
  "sh dev/run-rat.sh" !
}

Packager.defaultSettings
