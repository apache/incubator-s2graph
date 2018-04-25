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

package org.apache.s2graph.core.storage

import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.TraversalHelper._
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema.LabelMeta
import org.apache.s2graph.core.parsers.WhereParser
import org.apache.s2graph.core.utils.logger

class StorageIO(val graph: S2GraphLike, val serDe: StorageSerDe) {
  val dummyCursor: Array[Byte] = Array.empty

  /** Parsing Logic: parse from kv from Storage into Edge */
  def toEdge[K: CanSKeyValue](kv: K,
                              queryRequest: QueryRequest,
                              cacheElementOpt: Option[S2EdgeLike],
                              parentEdges: Seq[EdgeWithScore]): Option[S2EdgeLike] = {
    logger.debug(s"toEdge: $kv")

    try {
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val schemaVer = queryParam.label.schemaVersion
      val indexEdgeOpt = serDe.indexEdgeDeserializer(schemaVer).fromKeyValues(Seq(kv), cacheElementOpt)
      if (!queryOption.returnTree) indexEdgeOpt.map(indexEdge => indexEdge.copyParentEdges(parentEdges))
      else indexEdgeOpt
    } catch {
      case ex: Exception =>
        logger.error(s"Fail on toEdge: ${kv.toString}, ${queryRequest}", ex)
        None
    }
  }

  def toSnapshotEdge[K: CanSKeyValue](kv: K,
                                      queryRequest: QueryRequest,
                                      cacheElementOpt: Option[SnapshotEdge] = None,
                                      isInnerCall: Boolean,
                                      parentEdges: Seq[EdgeWithScore]): Option[S2EdgeLike] = {
    //        logger.debug(s"SnapshottoEdge: $kv")
    val queryParam = queryRequest.queryParam
    val schemaVer = queryParam.label.schemaVersion
    val snapshotEdgeOpt = serDe.snapshotEdgeDeserializer(schemaVer).fromKeyValues(Seq(kv), cacheElementOpt)

    if (isInnerCall) {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        val edge = snapshotEdge.toEdge.copyParentEdges(parentEdges)
        if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
        else None
      }
    } else {
      snapshotEdgeOpt.flatMap { snapshotEdge =>
        if (snapshotEdge.allPropsDeleted) None
        else {
          val edge = snapshotEdge.toEdge.copyParentEdges(parentEdges)
          if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
          else None
        }
      }
    }
  }

  def toEdges[K: CanSKeyValue](kvs: Seq[K],
                               queryRequest: QueryRequest,
                               prevScore: Double = 1.0,
                               isInnerCall: Boolean,
                               parentEdges: Seq[EdgeWithScore],
                               startOffset: Int = 0,
                               len: Int = Int.MaxValue): StepResult = {

    val toSKeyValue = implicitly[CanSKeyValue[K]].toSKeyValue _

    if (kvs.isEmpty) StepResult.Empty.copy(cursors = Seq(dummyCursor))
    else {
      val queryOption = queryRequest.query.queryOption
      val queryParam = queryRequest.queryParam
      val labelWeight = queryRequest.labelWeight
      val nextStepOpt = queryRequest.nextStepOpt
      val where = queryParam.where.get
      val label = queryParam.label
      val isDefaultTransformer = queryParam.edgeTransformer.isDefault
      val first = kvs.head
      val kv = first
      val schemaVer = queryParam.label.schemaVersion
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else serDe.indexEdgeDeserializer(schemaVer).fromKeyValues(Seq(kv), None)

      val (degreeEdges, keyValues) = cacheElementOpt match {
        case None => (Nil, kvs)
        case Some(cacheElement) =>
          val head = cacheElement
          if (!head.isDegree) (Nil, kvs)
          else (Seq(EdgeWithScore(head, 1.0, label)), kvs.tail)
      }

      val lastCursor: Seq[Array[Byte]] = Seq(if (keyValues.nonEmpty) toSKeyValue(keyValues(keyValues.length - 1)).row else dummyCursor)

      if (!queryOption.ignorePrevStepCache) {
        val edgeWithScores = for {
          (kv, idx) <- keyValues.zipWithIndex if idx >= startOffset && idx < startOffset + len
          edge <- (if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryRequest, None, isInnerCall, parentEdges) else toEdge(kv, queryRequest, cacheElementOpt, parentEdges)).toSeq
          if where == WhereParser.success || where.filter(edge)
          convertedEdge <- if (isDefaultTransformer) Seq(edge) else convertEdges(queryParam, edge, nextStepOpt)
        } yield {
          val score = queryParam.rank.score(edge)
          EdgeWithScore(convertedEdge, score, label)
        }
        StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = degreeEdges, cursors = lastCursor)
      } else {
        val degreeScore = 0.0

        val edgeWithScores = for {
          (kv, idx) <- keyValues.zipWithIndex if idx >= startOffset && idx < startOffset + len
          edge <- (if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryRequest, None, isInnerCall, parentEdges) else toEdge(kv, queryRequest, cacheElementOpt, parentEdges)).toSeq
          if where == WhereParser.success || where.filter(edge)
          convertedEdge <- if (isDefaultTransformer) Seq(edge) else convertEdges(queryParam, edge, nextStepOpt)
        } yield {
          val edgeScore = queryParam.rank.score(edge)
          val score = queryParam.scorePropagateOp match {
            case "plus" => edgeScore + prevScore
            case "divide" =>
              if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
              else edgeScore / (prevScore + queryParam.scorePropagateShrinkage)
            case _ => edgeScore * prevScore
          }
          val tsVal = processTimeDecay(queryParam, edge)
          val newScore = degreeScore + score
          EdgeWithScore(convertedEdge.copyParentEdges(parentEdges), score = newScore * labelWeight * tsVal, label = label)
        }

        val sampled =
          if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
          else edgeWithScores

        val normalized = if (queryParam.shouldNormalize) normalize(sampled) else sampled

        StepResult(edgeWithScores = normalized, grouped = Nil, degreeEdges = degreeEdges, cursors = lastCursor)
      }
    }
  }

  /** End Of Parse Logic */


  /** end of query */

  /** Mutation Builder */


  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] = {
    // skip sampling for delete operation
    val deleteMutations = edgeMutate.edgesToDeleteWithIndexOpt.flatMap { indexEdge =>
      serDe.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete, durability = indexEdge.label.durability))
    }

    val insertMutations = edgeMutate.edgesToInsertWithIndexOpt.flatMap { indexEdge =>
      serDe.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put, durability = indexEdge.label.durability))
    }

    deleteMutations ++ insertMutations
  }

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    edgeMutate.newSnapshotEdge.map(e => serDe.snapshotEdgeSerializer(e).toKeyValues.map(_.copy(durability = e.label.durability))).getOrElse(Nil)

  def increments(edgeMutate: EdgeMutate): (Seq[SKeyValue], Seq[SKeyValue]) = {
    (edgeMutate.edgesToDeleteWithIndexOptForDegree.isEmpty, edgeMutate.edgesToInsertWithIndexOptForDegree.isEmpty) match {
      case (true, true) =>
        /* when there is no need to update. shouldUpdate == false */
        Nil -> Nil

      case (true, false) =>
        /* no edges to delete but there is new edges to insert so increase degree by 1 */
        val (buffer, nonBuffer) = EdgeMutate.partitionBufferedIncrement(edgeMutate.edgesToInsertWithIndexOptForDegree)
        buffer.flatMap(buildIncrementsAsync(_)) -> nonBuffer.flatMap(buildIncrementsAsync(_))

      case (false, true) =>
        /* no edges to insert but there is old edges to delete so decrease degree by 1 */
        val (buffer, nonBuffer) = EdgeMutate.partitionBufferedIncrement(edgeMutate.edgesToDeleteWithIndexOptForDegree)
        buffer.flatMap(buildIncrementsAsync(_, -1)) -> nonBuffer.flatMap(buildIncrementsAsync(_, -1))

      case (false, false) =>
        /* update on existing edges so no change on degree */
        Nil -> Nil
    }
  }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.updatePropsWithTs()
    newProps.put(LabelMeta.degree.name, new S2Property(indexedEdge.toEdge, LabelMeta.degree, LabelMeta.degree.name, amount, indexedEdge.ts))
    val _indexedEdge = indexedEdge.copy(propsWithTs = newProps)
    serDe.indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment, durability = _indexedEdge.label.durability))
  }

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.updatePropsWithTs()
    newProps.put(LabelMeta.degree.name, new S2Property(indexedEdge.toEdge, LabelMeta.degree, LabelMeta.degree.name, amount, indexedEdge.ts))
    val _indexedEdge = indexedEdge.copy(propsWithTs = newProps)
    serDe.indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment, durability = _indexedEdge.label.durability))
  }

  //TODO: ServiceColumn do not have durability property yet.
  def buildDeleteBelongsToId(vertex: S2VertexLike): Seq[SKeyValue] = {
    val kvs = serDe.vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(S2Vertex.toPropKey(id)), operation = SKeyValue.Delete)
    }
  }

  def buildVertexPutsAsync(edge: S2EdgeLike): Seq[SKeyValue] = {
    val storeVertex = edge.innerLabel.extraOptions.get("storeVertex").map(_.as[Boolean]).getOrElse(false)

    if (storeVertex) {
      if (edge.getOp() == GraphUtil.operations("delete"))
        buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
      else
        serDe.vertexSerializer(edge.srcForVertex).toKeyValues ++ serDe.vertexSerializer(edge.tgtForVertex).toKeyValues
    } else {
      Seq.empty
    }
  }

  def buildDegreePuts(edge: S2EdgeLike, degreeVal: Long): Seq[SKeyValue] = {
    edge.propertyInner(LabelMeta.degree.name, degreeVal, edge.ts)
    val kvs = edge.edgesWithIndexValid.flatMap { indexEdge =>
      serDe.indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put, durability = indexEdge.label.durability))
    }

    kvs
  }

  def buildPutsAll(vertex: S2VertexLike): Seq[SKeyValue] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => serDe.vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
      case _ => serDe.vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }
  }
}
