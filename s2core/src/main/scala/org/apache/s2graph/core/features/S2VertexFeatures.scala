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
