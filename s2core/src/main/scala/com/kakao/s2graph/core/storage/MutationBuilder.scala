package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core._

import scala.collection.Seq
import scala.concurrent.ExecutionContext


abstract class MutationBuilder[T](storage: Storage)(implicit ex: ExecutionContext) {
  /** operation that needs to be supported by backend persistent storage system */
  def put(kvs: Seq[SKeyValue]): Seq[T]

  def increment(kvs: Seq[SKeyValue]): Seq[T]

  def delete(kvs: Seq[SKeyValue]): Seq[T]


  /** build mutation for backend persistent storage system */

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[T]

  def invertedEdgeMutations(edgeMutate: EdgeMutate): Seq[T]

  def increments(edgeMutate: EdgeMutate): Seq[T]

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T]

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T]

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[T]

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[T]

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[T]

  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[T]

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[T]

  def buildDeleteAsync(vertex: Vertex): Seq[T]

  def buildDeleteBelongsToId(vertex: Vertex): Seq[T]

  def buildVertexPutsAsync(edge: Edge): Seq[T]

  def buildPutsAll(vertex: Vertex): Seq[T] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync(vertex)
      case _ => buildPutsAsync(vertex)
    }
  }

}
