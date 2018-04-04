package org.apache.s2graph.s2jobs.serde.reader

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.serde.GraphElementReadable

class S2GraphCellReader extends GraphElementReadable[Seq[Cell]]{
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

      if (Bytes.equals(cf, SKeyValue.VertexCf)) {
        s2.defaultStorage.serDe.vertexDeserializer(schemaVer).fromKeyValues(kvs, None).map(_.asInstanceOf[GraphElement])
      } else if (Bytes.equals(cf, SKeyValue.EdgeCf)) {
        val indexEdgeOpt = s2.defaultStorage.serDe.indexEdgeDeserializer(schemaVer).fromKeyValues(kvs, None)
        if (indexEdgeOpt.isDefined) indexEdgeOpt.map(_.asInstanceOf[GraphElement])
        else {
          val snapshotEdgeOpt = s2.defaultStorage.serDe.snapshotEdgeDeserializer(schemaVer).fromKeyValues(kvs, None)
          if (snapshotEdgeOpt.isDefined) snapshotEdgeOpt.map(_.toEdge.asInstanceOf[GraphElement])
          else throw new IllegalStateException(s"column family indicate this is edge, but neither snapshot/index edge.")
        }
      } else throw new IllegalStateException(s"wrong column family. ${Bytes.toString(cf)}")
    }
  }
}
