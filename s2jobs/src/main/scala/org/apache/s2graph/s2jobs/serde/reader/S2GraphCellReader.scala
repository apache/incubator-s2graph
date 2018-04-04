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

package org.apache.s2graph.s2jobs.serde.reader

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.serde.GraphElementReadable

class S2GraphCellReader(elementType: String) extends GraphElementReadable[Seq[Cell]]{
  override def read(s2: S2Graph)(cells: Seq[Cell]): Option[GraphElement] = {
    if (cells.isEmpty) None
    else {
      //TODO:
      val cell = cells.head
      val schemaVer = HBaseType.DEFAULT_VERSION
      val cf = cell.getFamily

      val kvs = cells.map { cell =>
        new SKeyValue(Array.empty[Byte], cell.getRow, cell.getFamily, cell.getQualifier,
          cell.getValue, cell.getTimestamp, SKeyValue.Default)
      }
      elementType match {
        case "IndexEdge" =>
          if (!Bytes.equals(cf, SKeyValue.EdgeCf))
            throw new IllegalArgumentException(s"$elementType is provided by user, but actual column family differ as e")

          s2.defaultStorage.serDe.indexEdgeDeserializer(schemaVer)
            .fromKeyValues(kvs, None).map(_.asInstanceOf[GraphElement])
        case "SnapshotEdge" =>
          //TODO: replace this to use separate column family: SKeyValue.SnapshotEdgeCF
          if (!Bytes.equals(cf, SKeyValue.EdgeCf))
            throw new IllegalArgumentException(s"$elementType is provided by user, but actual column family differ as e")

          s2.defaultStorage.serDe.snapshotEdgeDeserializer(schemaVer)
            .fromKeyValues(kvs, None).map(_.toEdge.asInstanceOf[GraphElement])
        case "Vertex" =>
          if (!Bytes.equals(cf, SKeyValue.VertexCf))
            throw new IllegalArgumentException(s"$elementType is provided by user, but actual column family differ as v")

          s2.defaultStorage.serDe.vertexDeserializer(schemaVer)
            .fromKeyValues(kvs, None).map(_.asInstanceOf[GraphElement])
        case _ => throw new IllegalArgumentException(s"$elementType is not supported column family.")
      }
//      if (Bytes.equals(cf, SKeyValue.VertexCf)) {
//        s2.defaultStorage.serDe.vertexDeserializer(schemaVer).fromKeyValues(kvs, None).map(_.asInstanceOf[GraphElement])
//      } else if (Bytes.equals(cf, SKeyValue.EdgeCf)) {
//        val indexEdgeOpt = s2.defaultStorage.serDe.indexEdgeDeserializer(schemaVer).fromKeyValues(kvs, None)
//        if (indexEdgeOpt.isDefined) indexEdgeOpt.map(_.asInstanceOf[GraphElement])
//        else {
//          //TODO: Current version use same column family for snapshotEdge and indexEdge.
//
//          val snapshotEdgeOpt = s2.defaultStorage.serDe.snapshotEdgeDeserializer(schemaVer).fromKeyValues(kvs, None)
////          if (snapshotEdgeOpt.isDefined) snapshotEdgeOpt.map(_.toEdge.asInstanceOf[GraphElement])
////          else throw new IllegalStateException(s"column family indicate this is edge, but neither snapshot/index edge.")
//          None
//        }
//      } else throw new IllegalStateException(s"wrong column family. ${Bytes.toString(cf)}")
    }
  }
}
