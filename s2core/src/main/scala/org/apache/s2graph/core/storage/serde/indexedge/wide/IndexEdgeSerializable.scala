package org.apache.s2graph.core.storage.serde.indexedge.wide

import com.kakao.s2graph.core.storage.StorageSerializable
import com.kakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.s2graph.core.storage.{Serializable, SKeyValue, StorageSerializable}
import org.apache.s2graph.core.types.VertexId

import scala.Serializable


class IndexEdgeSerializable(indexEdge: IndexEdge) extends Serializable[IndexEdge] {
   import StorageSerializable._

   val label = indexEdge.label
   val table = label.hbaseTableName.getBytes()
   val cf = Serializable.edgeCf

   val idxPropsMap = indexEdge.orders.toMap
   val idxPropsBytes = propsToBytes(indexEdge.orders)

   override def toKeyValues: Seq[SKeyValue] = {
     val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
     val labelWithDirBytes = indexEdge.labelWithDir.bytes
     val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

     val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
     //    logger.error(s"${row.toList}\n${srcIdBytes.toList}\n${labelWithDirBytes.toList}\n${labelIndexSeqWithIsInvertedBytes.toList}")
     val tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes
     val qualifier =
       if (indexEdge.degreeEdge) Array.empty[Byte]
       else {
         if (indexEdge.op == GraphUtil.operations("incrementCount")) {
           Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexEdge.op))
         } else {
           idxPropsMap.get(LabelMeta.toSeq) match {
             case None => Bytes.add(idxPropsBytes, tgtIdBytes)
             case Some(vId) => idxPropsBytes
           }
         }
       }


     val value =
       if (indexEdge.degreeEdge)
         Bytes.toBytes(indexEdge.propsWithTs(LabelMeta.degreeSeq).innerVal.toString().toLong)
       else if (indexEdge.op == GraphUtil.operations("incrementCount"))
         Bytes.toBytes(indexEdge.propsWithTs(LabelMeta.countSeq).innerVal.toString().toLong)
       else propsToKeyValues(indexEdge.metas.toSeq)

     val kv = SKeyValue(table, row, cf, qualifier, value, indexEdge.version)

     Seq(kv)
   }
 }
