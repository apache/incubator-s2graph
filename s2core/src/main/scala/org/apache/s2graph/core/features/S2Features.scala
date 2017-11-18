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

class S2Features extends Features {
  import org.apache.s2graph.core.{features => FS}
  override def edge(): Features.EdgeFeatures = new FS.S2EdgeFeatures

  override def graph(): Features.GraphFeatures = new FS.S2GraphFeatures

  override def supports(featureClass: Class[_ <: Features.FeatureSet], feature: String): Boolean =
    super.supports(featureClass, feature)

  override def vertex(): Features.VertexFeatures = new FS.S2VertexFeatures

  override def toString: String = {
    s"FEATURES:\nEdgeFeatures:${edge}\nGraphFeatures:${graph}\nVertexFeatures:${vertex}"
  }
}
