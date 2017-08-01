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

// init graph
graph = S2Graph.open(new BaseConfiguration())

// 0. import
import static org.apache.s2graph.core.Management.*

// 1. create service
session = graph.dbSession()
serviceName = "s2graph"
cluster = "localhost"
hTableName = "s2graph"
preSplitSize = 0
hTableTTL = -1
compressionAlgorithm = "gz"

service = graph.management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)

// 2. create vertex schema
columnName = "user"
columnType = "integer"
props = [newProp("name", "-", "string"), newProp("age", "-1", "integer")]
schemaVersion = "v3"
user = graph.management.createServiceColumn(serviceName, columnName, columnType, props, schemaVersion)

// 2.1 (optional) global vertex index.
graph.management.buildGlobalVertexIndex("global_vertex_index", ["name", "age"])

// 3. create VertexId
v1Id = graph.newVertexId(serviceName, columnName, 20)
v2Id = graph.newVertexId(serviceName, columnName, 30)

shon = graph.addVertex(T.id, v1Id, "name", "shon", "age", 35)
dun = graph.addVertex(T.id, v2Id, "name", "dun", "age", 36)

// 4. friends label
labelName = "friend_"
srcColumn = user
tgtColumn = user
isDirected = true
indices = []
props = [newProp("since", "-", "string")]
consistencyLevel = "strong"
hTableName = "s2graph"
hTableTTL = -1
options = null

friend = graph.management.createLabel(labelName, srcColumn, tgtColumn,
        isDirected, serviceName, indices, props, consistencyLevel,
        hTableName, hTableTTL, schemaVersion, compressionAlgorithm, options)

shon.addEdge(labelName, dun, "since", "2017-01-01")

t = graph.traversal()


println "All Edges"
println t.E().toList()

println "All Vertices"
println t.V().toList()

println "Specific Edge"
println t.V().has("name", "shon").out().toList()