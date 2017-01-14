package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2PropertyFeatures extends S2DataTypeFeatures with Features.PropertyFeatures {
  override def supportsProperties(): Boolean = true
}
