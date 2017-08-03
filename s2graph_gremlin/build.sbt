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

import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

import Common._

val gremlin_version = Common.tinkerpopVersion

name := "s2graph-gremlin"
version := version.value

scalacOptions ++= Seq("-deprecation")


projectDependencies := Seq(
  (projectID in "s2core").value exclude("org.mortbay.jetty", "*") exclude("javax.xml.stream", "*") exclude("javax.servlet", "*")
)


libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1" force(), // use this old version of guava to avoid incompatibility
  "org.apache.hadoop" % "hadoop-mapreduce-client-app" % Common.hadoopVersion excludeLogging(),
  "org.apache.tinkerpop" % "gremlin-groovy" % gremlin_version excludeLogging()
)


pomIncludeRepository := { (repo: MavenRepository) => false}
pomPostProcess := { (node: XmlNode) =>
  new RuleTransformer(new RewriteRule {
    override def transform(node: XmlNode): XmlNodeSeq = node match {
      case e: Elem if e.label == "dependency" =>
        Comment(s"provided dependency $organization#$artifact;$version has been omitted")
      case _ => node
    }
  }).transform(node).head
}


publishMavenStyle := true
publishArtifact in (Test, packageSrc) := false

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

autoScalaLibrary := false
crossPaths := false

mergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "services", ps @ _*) => MergeStrategy.first
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case PathList("license", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}

parallelExecution in Test := false

publishArtifact in (Compile, packageBin) := false
publishArtifact in (Compile, packageSrc) := false
publishArtifact in (Compile, packageDoc) := false

addArtifact(artifact in (Compile,  assembly), assembly)

