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

package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality

class S2VertexFeatures extends S2ElementFeatures with Features.VertexFeatures {
  override def supportsAddVertices(): Boolean = true

  override def supportsRemoveVertices(): Boolean = true

  override def getCardinality(key: String): Cardinality = Cardinality.single

  override def supportsMultiProperties(): Boolean = false

  override def supportsMetaProperties(): Boolean = false

  override def properties(): Features.VertexPropertyFeatures = new S2VertexPropertyFeatures()
}
