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

name := "s2counter-core"

scalacOptions ++= Seq("-feature", "-deprecation", "-language:existentials")

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1" force (), // use this old version of guava to avoid incompatibility
  "com.typesafe.play" %% "play-ws" % playVersion excludeLogging (),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeLogging,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion excludeLogging,
  "redis.clients" % "jedis" % "2.6.0",
  "org.specs2" %% "specs2-core" % specs2Version % "test"
)
