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
