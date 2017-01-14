package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2EdgeFeatures extends S2ElementFeatures with Features.EdgeFeatures {
  override def supportsRemoveEdges(): Boolean = true

  override def supportsAddEdges(): Boolean = true

  override def properties(): Features.EdgePropertyFeatures = new S2EdgePropertyFeatures
}
