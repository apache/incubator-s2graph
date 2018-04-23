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
  override def read(s2: S2Graph)(cells: Seq[Cell]): Seq[GraphElement] = {
    val schemaVer = HBaseType.DEFAULT_VERSION
    val kvs = cells.map { cell =>
      new SKeyValue(Array.empty[Byte], cell.getRow, cell.getFamily, cell.getQualifier,
        cell.getValue, cell.getTimestamp, SKeyValue.Default)
    }

    elementType.toLowerCase match {
      case "vertex" | "v" =>
        s2.defaultStorage.serDe.vertexDeserializer(schemaVer)
          .fromKeyValues(kvs, None).map(_.asInstanceOf[GraphElement]).toSeq
      case "indexedge" | "ie" =>
        kvs.flatMap { kv =>
          s2.defaultStorage.serDe.indexEdgeDeserializer(schemaVer)
            .fromKeyValues(Seq(kv), None).map(_.asInstanceOf[GraphElement])
        }
      case "snapshotedge" | "se" =>
        kvs.flatMap { kv =>
          s2.defaultStorage.serDe.snapshotEdgeDeserializer(schemaVer)
            .fromKeyValues(Seq(kv), None).map(_.asInstanceOf[GraphElement])
        }
      case _ => throw new IllegalArgumentException(s"$elementType is not supported.")
    }
  }
}
