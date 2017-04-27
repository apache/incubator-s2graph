package org.apache.s2graph.core


import org.apache.s2graph.core.io.S2GraphSimpleModule
import org.apache.s2graph.core.types.VertexId
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo
import org.apache.tinkerpop.shaded.kryo.io.{Input, Output}
import org.apache.tinkerpop.shaded.kryo.{Kryo, Serializer}

object S2GraphIoRegistry {
  lazy val instance = new S2GraphIoRegistry
}

class S2GraphIoRegistry extends AbstractIoRegistry {

  //  val simpleModule = new S2GraphSimpleModule
  register(classOf[GraphSONIo], null, S2GraphSimpleModule.getInstance())
  //  register(classOf[GraphSONIo], null, simpleModule)

  register(classOf[GryoIo], classOf[S2VertexPropertyId], new S2VertexPropertyIdKryoSerializer)
  register(classOf[GryoIo], classOf[VertexId], new VertexIdKryoSerializer)
  register(classOf[GryoIo], classOf[EdgeId], new EdgeIdKryoSerializer)

  class S2VertexPropertyIdKryoSerializer extends Serializer[S2VertexPropertyId] {
    override def read(kryo: Kryo, input: Input, aClass: Class[S2VertexPropertyId]): S2VertexPropertyId = {
      S2VertexPropertyId.fromString(input.readString())
    }

    override def write(kryo: Kryo, output: Output, t: S2VertexPropertyId): Unit = {
      output.writeString(t.toString)
    }
  }

  class VertexIdKryoSerializer extends Serializer[VertexId] {
    override def read(kryo: Kryo, input: Input, aClass: Class[VertexId]): VertexId = {
      VertexId.fromString(input.readString())
    }

    override def write(kryo: Kryo, output: Output, t: VertexId): Unit = {
      output.writeString(t.toString())
    }
  }


  class EdgeIdKryoSerializer extends Serializer[EdgeId] {
    override def read(kryo: Kryo, input: Input, aClass: Class[EdgeId]): EdgeId = {
      EdgeId.fromString(input.readString())
    }

    override def write(kryo: Kryo, output: Output, t: EdgeId): Unit = {
      output.writeString(t.toString)
    }
  }


}
