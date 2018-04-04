package org.apache.s2graph.s2jobs.serde.reader

import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.serde.GraphElementReadable

class IdentityReader extends GraphElementReadable[GraphElement] {
  override def read(graph: S2Graph)(data: GraphElement): Option[GraphElement] =
    Option(data)
}
