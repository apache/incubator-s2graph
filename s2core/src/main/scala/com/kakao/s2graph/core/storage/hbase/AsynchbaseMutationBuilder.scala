package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{DeleteRequest, AtomicIncrementRequest, PutRequest, HBaseRpc}

import scala.collection.Seq
import scala.concurrent.ExecutionContext

class AsynchbaseMutationBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends MutationBuilder[HBaseRpc](storage) {

  def put(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }

  def increment(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }

  def delete(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
    kvs.map { kv =>
      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
    }

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[HBaseRpc] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap(edge => buildDeletesAsync(edge))
    val insertMutations = edgeMutate.edgesToInsert.flatMap(edge => buildPutsAsync(edge))

    deleteMutations ++ insertMutations
  }

  def invertedEdgeMutations(edgeMutate: EdgeMutate): Seq[HBaseRpc] =
    edgeMutate.newInvertedEdge.map(e => buildDeleteAsync(e)).getOrElse(Nil)


  def increments(edgeMutate: EdgeMutate): Seq[HBaseRpc] = {
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        List.empty[AtomicIncrementRequest]
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        List.empty[AtomicIncrementRequest]
    }
  }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[HBaseRpc] =
    storage.indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(qualifier = Array.empty[Byte], value = Bytes.toBytes(amount))
        increment(Seq(copiedKV))
    }


  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[HBaseRpc] =
    storage.indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(value = Bytes.toBytes(amount))
        increment(Seq(copiedKV))
    }

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[HBaseRpc] =
    delete(storage.indexEdgeSerializer(indexedEdge).toKeyValues)

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[HBaseRpc] =
    put(storage.indexEdgeSerializer(indexedEdge).toKeyValues)

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[HBaseRpc] =
    put(storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[HBaseRpc] =
    delete(storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[HBaseRpc] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    put(kvs)
  }


  def buildDeleteAsync(vertex: Vertex): Seq[HBaseRpc] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    delete(Seq(kv.copy(qualifier = null)))
  }

  def buildDeleteBelongsToId(vertex: Vertex): Seq[HBaseRpc] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    val kv = kvs.head

    import org.apache.hadoop.hbase.util.Bytes
    val newKVs = vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)))
    }
    delete(newKVs)
  }

  def buildVertexPutsAsync(edge: Edge): Seq[HBaseRpc] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutsAsync(edge.srcForVertex) ++ buildPutsAsync(edge.tgtForVertex)
}
