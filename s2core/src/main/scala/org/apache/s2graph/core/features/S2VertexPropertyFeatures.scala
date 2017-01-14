package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2VertexPropertyFeatures extends S2PropertyFeatures with Features.VertexPropertyFeatures {

  override def supportsStringIds(): Boolean = true

  override def supportsUserSuppliedIds(): Boolean = true

  override def supportsAddProperty(): Boolean = true

  override def willAllowId(id: scala.Any): Boolean = true

  override def supportsNumericIds(): Boolean = false

  override def supportsRemoveProperty(): Boolean = true

  override def supportsUuidIds(): Boolean = false

  override def supportsCustomIds(): Boolean = false

  override def supportsAnyIds(): Boolean = false
}
