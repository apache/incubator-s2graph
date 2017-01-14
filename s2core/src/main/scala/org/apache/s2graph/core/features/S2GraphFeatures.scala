package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features
import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures


class S2GraphFeatures extends GraphFeatures {
  override def supportsComputer(): Boolean = false

  override def supportsThreadedTransactions(): Boolean = false

  override def supportsTransactions(): Boolean = false

  override def supportsPersistence(): Boolean = true

  override def variables(): Features.VariableFeatures = super.variables()

  override def supportsConcurrentAccess(): Boolean = false
}
