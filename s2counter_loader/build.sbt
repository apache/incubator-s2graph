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

name := "s2counter-loader"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "javax.servlet" % "javax.servlet-api" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

// force specific library version
libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "16.0.1"
)

crossScalaVersions := Seq("2.10.6")

fork := true

transitiveClassifiers ++= Seq()

mergeStrategy in assembly := {
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}

mainClass in (Compile) := None