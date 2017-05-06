/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
