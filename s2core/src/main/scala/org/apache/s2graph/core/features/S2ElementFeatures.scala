package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

abstract class S2ElementFeatures extends Features.ElementFeatures {
  override def supportsStringIds(): Boolean = false

  override def supportsCustomIds(): Boolean = true

  override def supportsUuidIds(): Boolean = false

  override def supportsAddProperty(): Boolean = true

  override def supportsRemoveProperty(): Boolean = true

  override def supportsUserSuppliedIds(): Boolean = true

  override def supportsAnyIds(): Boolean = false

  override def supportsNumericIds(): Boolean = false

  override def willAllowId(id: scala.Any): Boolean = true
//  override def willAllowId(id: scala.Any): Boolean = super.willAllowId(id)
}
