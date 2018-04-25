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

package org.apache.s2graph.core

import org.apache.commons.configuration.BaseConfiguration
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core.schema.{ColumnMeta, ServiceColumn}
import org.apache.s2graph.core.types.HBaseType
import org.apache.tinkerpop.gremlin.structure.T

object S2GraphFactory {

  def generateClassic(g: S2Graph): Unit = {
    val marko = g.addVertex(T.id, Int.box(1), "name", "marko", "age", Int.box(29))
    val vadas = g.addVertex(T.id, Int.box(2), "name", "vadas", "age", Int.box(27))
    val lop = g.addVertex(T.id, Int.box(3), "name", "lop", "lang", "java")
    val josh = g.addVertex(T.id, Int.box(4), "name", "josh", "age", Int.box(32))
    val ripple = g.addVertex(T.id, Int.box(5), "name", "ripple", "lang", "java")
    val peter = g.addVertex(T.id, Int.box(6), "name", "peter", "age", Int.box(35))
    marko.addEdge("knows", vadas, T.id, Int.box(7), "weight", Float.box(0.5f))
    marko.addEdge("knows", josh, T.id, Int.box(8), "weight", Float.box(1.0f))
    marko.addEdge("created", lop, T.id, Int.box(9), "weight", Float.box(0.4f))
    josh.addEdge("created", ripple, T.id, Int.box(10), "weight", Float.box(1.0f))
    josh.addEdge("created", lop, T.id, Int.box(11), "weight", Float.box(0.4f))
    peter.addEdge("created", lop, T.id, Int.box(12), "weight", Float.box(0.2f))
  }

  def generateModern(g: S2Graph): Unit = {
    val marko = g.addVertex(T.id, Int.box(1), T.label, "person", "name", "marko", "age", Int.box(29))
    val vadas = g.addVertex(T.id, Int.box(2), T.label, "person", "name", "vadas", "age", Int.box(27))
    val lop = g.addVertex(T.id, Int.box(3), T.label, "software", "name", "lop", "lang", "java")
    val josh = g.addVertex(T.id, Int.box(4), T.label, "person", "name", "josh", "age", Int.box(32))
    val ripple = g.addVertex(T.id, Int.box(5), T.label, "software", "name", "ripple", "lang", "java")
    val peter = g.addVertex(T.id, Int.box(6), T.label, "person", "name", "peter", "age", Int.box(35))

    marko.addEdge("knows", vadas, T.id, Int.box(7), "weight", Double.box(0.5d))
    marko.addEdge("knows", josh, T.id, Int.box(8), "weight", Double.box(1.0d))
    marko.addEdge("created", lop, T.id, Int.box(9), "weight", Double.box(0.4d))
    josh.addEdge("created", ripple, T.id, Int.box(10), "weight", Double.box(1.0d))
    josh.addEdge("created", lop, T.id, Int.box(11), "weight", Double.box(0.4d))
    peter.addEdge("created", lop, T.id, Int.box(12), "weight", Double.box(0.2d))
  }

  def initDefaultSchema(graph: S2Graph): Unit = {
    val management = graph.management

    //    Management.deleteService(DefaultServiceName)
    val DefaultService = management.createService(DefaultServiceName, "localhost", "s2graph", 0, None).get

    //    Management.deleteColumn(DefaultServiceName, DefaultColumnName)
    val DefaultColumn = ServiceColumn.findOrInsert(DefaultService.id.get, DefaultColumnName, Some("integer"), HBaseType.DEFAULT_VERSION, useCache = false)

    val DefaultColumnMetas = {
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "test", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "name", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "age", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "lang", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "oid", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "communityIndex", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "testing", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "string", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "boolean", "boolean", "true", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "long", "long", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "float", "float", "0.0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "double", "double", "0.0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "integer", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "aKey", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "x", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "y", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "location", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "status", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "myId", "integer", "0", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "acl", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "some", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "this", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "that", "string", "-", storeInGlobalIndex = true, useCache = false)
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "any", "string", "-", storeInGlobalIndex = true, useCache = false)
    }

    //    Management.deleteLabel("_s2graph")
    val DefaultLabel = management.createLabel("_s2graph", DefaultService.serviceName, DefaultColumn.columnName, DefaultColumn.columnType,
      DefaultService.serviceName, DefaultColumn.columnName, DefaultColumn.columnType, true, DefaultService.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
  }

  def initModernSchema(g: S2Graph): Unit = {
    val mnt = g.management
    val softwareColumn = Management.createServiceColumn(S2Graph.DefaultServiceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
    val personColumn = Management.createServiceColumn(S2Graph.DefaultServiceName, "person", "integer",
      Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))

    val knows = mnt.createLabel("knows",
      S2Graph.DefaultServiceName, "person", "integer",
      S2Graph.DefaultServiceName, "person", "integer",
      true, S2Graph.DefaultServiceName, Nil, Seq(Prop("weight", "0.0", "double"), Prop("year", "0", "integer")), consistencyLevel = "strong", None, None)

    val created = mnt.createLabel("created",
      S2Graph.DefaultServiceName, "person", "integer",
      S2Graph.DefaultServiceName, "software", "integer",
      true, S2Graph.DefaultServiceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)
  }

  def cleanupDefaultSchema(): Unit = {
    val columnNames = Set(S2Graph.DefaultColumnName, "person", "software", "product", "dog",
      "animal", "song", "artist", "STEPHEN")

    val labelNames = Set(S2Graph.DefaultLabelName, "knows", "created", "bought", "test", "self", "friends", "friend", "hate", "collaborator",
      "test1", "test2", "test3", "pets", "walks", "hates", "link",
      "codeveloper", "createdBy", "existsWith", "writtenBy", "sungBy", "followedBy", "uses", "likes", "foo", "bar")

    columnNames.foreach { columnName =>
      Management.deleteColumn(S2Graph.DefaultServiceName, columnName)
    }
    labelNames.foreach { labelName =>
      Management.deleteLabel(labelName)
    }
  }
}
