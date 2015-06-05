package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by shon on 6/5/15.
 */
object CompositeIdV1 {
  import CompositeId._
  def apply(bytes: Array[Byte], offset: Int, isEdge: Boolean, useHash: Boolean): CompositeId = {
    var pos = offset
    if (useHash) {
      // skip over murmur hash
      pos += GraphUtil.bytesForMurMurHash
    }
    val innerId = InnerValV1(bytes, pos)
    pos += innerId.bytes.length
    if (isEdge) {
      new CompositeIdV1(defaultColId, innerId, true, useHash)
    } else {
      val cId = Bytes.toInt(bytes, pos, 4)
      new CompositeIdV1(cId, innerId, false, useHash)
    }
  }
}
class CompositeIdV1(override val colId: Int,
                    override val innerId: InnerVal,
                    override val isEdge: Boolean,
                    override val useHash: Boolean)
  extends CompositeId(colId, innerId, isEdge, useHash) {


}
