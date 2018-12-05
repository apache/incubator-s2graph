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

import Common._

name := "s2jobs"

scalacOptions ++= Seq("-deprecation")

projectDependencies := Seq(
  (projectID in "s2core").value exclude("org.mortbay.jetty", "j*") exclude("javax.xml.stream", "s*") exclude("javax.servlet", "s*") exclude("javax.servlet", "j*")
)

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.spark" %% "spark-core" % spark2Version,
  "org.apache.spark" %% "spark-streaming" % spark2Version % "provided",
  "org.apache.spark" %% "spark-hive" % spark2Version % "provided",
  "org.apache.spark" %% "spark-mllib" % spark2Version,
  "org.specs2" %% "specs2-core" % specs2Version % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.hadoop" % "hadoop-distcp" % hadoopVersion,
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % elastic4sVersion,
  "com.github.scopt" %% "scopt" % "3.7.0",
  "io.thekraken" % "grok" % "0.1.5",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test
)

crossScalaVersions := Seq("2.10.6")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "guava-16.0.1.jar"}
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "org.apache.s2graph.shade.google.protobuf.@1").inAll
)

projectDependencies := Seq(
  (projectID in "s2core").value exclude("org.mortbay.jetty", "j*") exclude("javax.xml.stream", "s*") exclude("javax.servlet", "s*") exclude("net.jpountz.lz4", "lz4")
)

test in assembly := {}

parallelExecution in Test := false

mainClass in (Compile) := None
