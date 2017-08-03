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

// init schema
graph = S2Graph.open(new BaseConfiguration())
S2GraphFactory.initDefaultSchema(graph)

S2GraphFactory.initModernSchema(graph)
S2GraphFactory.generateModern(graph)

// traversal
t = graph.traversal()
t.E()

shon = graph.addVertex(T.id, 10, T.label, "person", "name", "shon", "age", 35)
s2graph = graph.addVertex(T.id, 11, T.label, "software", "name", "s2graph", "lang", "scala")

created = shon.addEdge("created", s2graph, "_timestamp", 10, "weight", 0.1)

t.V().has("name", "shon").out()
