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

name := """s2core"""

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.play" %% "play-json" % Common.playVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.hbase" % "hbase-client" % Common.hbaseVersion exclude("org.slf4j", "*"),
  "org.apache.hbase" % "hbase-common" % Common.hbaseVersion exclude("org.slf4j", "*"),
  "org.apache.hbase" % "hbase-server" % Common.hbaseVersion exclude("org.slf4j", "*") exclude("com.google.protobuf", "*"),
  "org.apache.hbase" % "hbase-hadoop-compat" % Common.hbaseVersion exclude("org.slf4j", "*"),
  "org.apache.hbase" % "hbase-hadoop2-compat" % Common.hbaseVersion exclude("org.slf4j", "*"),
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0" exclude("org.slf4j", "*") exclude("com.sun.jdmk", "*") exclude("com.sun.jmx", "*") exclude("javax.jms", "*"),
  "commons-pool" % "commons-pool" % "1.6",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalikejdbc" %% "scalikejdbc" % "2.1.+",
  "mysql" % "mysql-connector-java" % "5.1.28",
  "com.h2database" % "h2" % "1.4.192",
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.4",
  "org.hbase" % "asynchbase" % "1.7.2-S2GRAPH" from "https://github.com/SteamShon/asynchbase/raw/mvn-repo/org/hbase/asynchbase/1.7.2-S2GRAPH/asynchbase-1.7.2-S2GRAPH-jar-with-dependencies.jar"
)

libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    // if scala 2.11+ is used, add dependency on scala-xml mqqodule
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value ++ Seq(
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
      )
    case _ =>
      libraryDependencies.value
  }
}
