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

import sbt._

object Common {
  val sparkVersion = "1.4.1"
  val spark2Version = "2.3.0"
  val playVersion = "2.5.9"
  val specs2Version = "3.8.5"

  val hbaseVersion = "1.2.6.1"
  val asynchbaseVersion = "1.7.2"
  val hadoopVersion = "2.7.3"
  val tinkerpopVersion = "3.2.5"

  val elastic4sVersion = "6.2.4"

  val KafkaVersion = "0.10.2.1"

  val rocksVersion = "5.11.3"

  val annoy4sVersion = "0.6.0"

  val tensorflowVersion = "1.7.0"

  val log4jVersion = "1.2.17"

  val slf4jBindingVersion = "1.8.0-beta2"

  /** use Log4j 1.2.17 as the SLF4j backend in runtime, with bridging libraries to forward JCL and JUL logs to SLF4j */
  val loggingRuntime = Seq(
    "log4j" % "log4j" % log4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jBindingVersion,
    "org.slf4j" % "jcl-over-slf4j" % slf4jBindingVersion,
    "org.slf4j" % "jul-to-slf4j" % slf4jBindingVersion
  ).flatMap(dep => Seq(dep % "test", dep % "runtime"))

  /** rules to exclude logging backends and bridging libraries from dependency */
  val loggingExcludes = Seq(
    ExclusionRule("commons-logging", "commons-logging"),
    ExclusionRule("log4j", "log4j"),
    ExclusionRule("ch.qos.logback", "logback-classic"),
    ExclusionRule("ch.qos.logback", "logback-core"),
    ExclusionRule("org.slf4j", "jcl-over-slf4j"),
    ExclusionRule("org.slf4j", "log4j-over-slf4j"),
    ExclusionRule("org.slf4j", "slf4j-log4j12"),
    ExclusionRule("org.slf4j", "jul-to-slf4j")
  )

  implicit class LoggingExcluder(moduleId: ModuleID) {
    def excludeLogging(): ModuleID = moduleId.excludeAll(loggingExcludes: _*)
  }
}
