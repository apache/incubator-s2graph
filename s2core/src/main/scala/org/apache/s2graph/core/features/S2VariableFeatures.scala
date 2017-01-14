package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2VariableFeatures extends S2DataTypeFeatures with Features.VariableFeatures {
  override def supportsVariables(): Boolean = false
}
