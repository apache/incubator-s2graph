package org.apache.s2graph.graphql.resolver

import org.apache.s2graph.core.S2VertexLike
import org.apache.s2graph.graphql.repository.GraphRepository

object Resolver {
  def vertexResolver(v: S2VertexLike)(implicit repo: GraphRepository) {
  }
}
