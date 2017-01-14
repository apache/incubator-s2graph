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

name := """s2core"""

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.play" %% "play-json" % playVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.hbase" % "hbase-client" % hbaseVersion excludeLogging(),
  "org.apache.hbase" % "hbase-common" % hbaseVersion excludeLogging(),
  "org.apache.hbase" % "hbase-server" % hbaseVersion excludeLogging() exclude("com.google.protobuf", "protobuf*"),
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion excludeLogging(),
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion excludeLogging(),
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0" excludeLogging() exclude("com.sun.jdmk", "j*") exclude("com.sun.jmx", "j*") exclude("javax.jms", "j*"),
  "commons-pool" % "commons-pool" % "1.6",
  "org.scalikejdbc" %% "scalikejdbc" % "2.1.4",
  "com.h2database" % "h2" % "1.4.192",
  "com.stumbleupon" % "async" % "1.4.1",
  "io.netty" % "netty" % "3.9.4.Final" force(),
  "org.hbase" % "asynchbase" % "1.7.2" excludeLogging(),
  "net.bytebuddy" % "byte-buddy" % "1.4.26",
  "org.apache.tinkerpop" % "gremlin-core" % tinkerpopVersion,
  "org.apache.tinkerpop" % "gremlin-test" % tinkerpopVersion % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.specs2" %% "specs2-core" % specs2Version % "test",
  "mysql" % "mysql-connector-java" % "5.1.40"
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
