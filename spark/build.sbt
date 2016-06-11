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

name := "s2spark"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "com.typesafe.play" %% "play-json" % Common.playVersion,
  "org.specs2" %% "specs2-core" % "3.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
