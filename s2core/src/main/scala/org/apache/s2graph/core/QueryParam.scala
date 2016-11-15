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

import com.google.common.hash.Hashing
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.DuplicatePolicy.DuplicatePolicy
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.parsers.{Where, WhereParser}
import org.apache.s2graph.core.storage.StorageSerializable._
import org.apache.s2graph.core.types.{InnerVal, InnerValLike, InnerValLikeWithTs, LabelWithDirection}
import org.hbase.async.ColumnRangeFilter
import play.api.libs.json.{JsNull, JsValue, Json}

import scala.util.{Success, Try}

object Query {
  val initialScore = 1.0
  lazy val empty = Query()

  def toQuery(srcVertices: Seq[Vertex], queryParam: QueryParam) = Query(srcVertices, Vector(Step(List(queryParam))))

}

case class MinShouldMatchParam(prop: String, count: Int, terms: Set[Any])

object GroupBy {
  val Empty = GroupBy()
}
case class GroupBy(keys: Seq[String] = Nil,
                   limit: Int = Int.MaxValue,
                   minShouldMatch: Option[MinShouldMatchParam]= None)

case class MultiQuery(queries: Seq[S2Query],
                      weights: Seq[Double],
                      queryOption: QueryOption,
                      jsonQuery: JsValue = JsNull)

object QueryOption {
  val DefaultAscendingVals: Seq[Boolean] = Seq(false, false)
}

case class QueryOption(removeCycle: Boolean = false,
                       selectColumns: Seq[String] = Seq.empty,
                       groupBy: GroupBy = GroupBy.Empty,
                       orderByColumns: Seq[(String, Boolean)] = Seq.empty,
                       filterOutQuery: Option[S2Query] = None,
                       filterOutFields: Seq[String] = Seq(LabelMeta.to.name),
                       withScore: Boolean = true,
                       returnTree: Boolean = false,
                       limitOpt: Option[Int] = None,
                       returnAgg: Boolean = true,
                       scoreThreshold: Double = Double.MinValue,
                       returnDegree: Boolean = true,
                       impIdOpt: Option[String] = None) {
  val orderByKeys = orderByColumns.map(_._1)
  val ascendingVals = orderByColumns.map(_._2)
  val selectColumnsMap = selectColumns.map { c => c -> true } .toMap
  val scoreFieldIdx = orderByKeys.zipWithIndex.find(t => t._1 == "score").map(_._2).getOrElse(-1)
  val (edgeSelectColumns, propsSelectColumns) = selectColumns.partition(c => LabelMeta.defaultRequiredMetaNames.contains(c))
  /** */
  val edgeSelectColumnsFiltered = edgeSelectColumns
//  val edgeSelectColumnsFiltered = edgeSelectColumns.filterNot(c => groupBy.keys.contains(c))
  lazy val cacheKeyBytes: Array[Byte] = {
    val selectBytes = Bytes.toBytes(selectColumns.toString)
    val groupBytes = Bytes.toBytes(groupBy.keys.toString)
    val orderByBytes = Bytes.toBytes(orderByColumns.toString)
    val filterOutBytes = filterOutQuery.map(_.cacheKeyBytes).getOrElse(Array.empty[Byte])
    val returnTreeBytes = Bytes.toBytes(returnTree)

    Seq(selectBytes, groupBytes, orderByBytes, filterOutBytes, returnTreeBytes).foldLeft(Array.empty[Byte])(Bytes.add)
  }

}

case class S2Query(vertices: Seq[Vertex] = Seq.empty[Vertex],
                   steps: IndexedSeq[Step] = Vector.empty[Step],
                   queryOption: QueryOption = QueryOption(),
                   jsonQuery: JsValue = JsNull) {
  lazy val innerQuery = Query(vertices, steps, queryOption, jsonQuery)

  lazy val cacheKeyBytes = steps.map(_.cacheKeyBytes).foldLeft(queryOption.cacheKeyBytes)(Bytes.add)
}

case class Query(vertices: Seq[Vertex] = Seq.empty[Vertex],
                 steps: IndexedSeq[Step] = Vector.empty[Step],
                 queryOption: QueryOption = QueryOption(),
                 jsonQuery: JsValue = JsNull) {

  lazy val cacheKeyBytes: Array[Byte] = {
    val selectBytes = Bytes.toBytes(queryOption.selectColumns.toString)
    val groupBytes = Bytes.toBytes(queryOption.groupBy.keys.toString)
    val orderByBytes = Bytes.toBytes(queryOption.orderByColumns.toString)
    val filterOutBytes = queryOption.filterOutQuery.map(_.cacheKeyBytes).getOrElse(Array.empty[Byte])
    val returnTreeBytes = Bytes.toBytes(queryOption.returnTree)

    Seq(selectBytes, groupBytes, orderByBytes, filterOutBytes, returnTreeBytes).foldLeft(Array.empty[Byte])(Bytes.add)
  }

  lazy val fullCacheKeyBytes: Array[Byte] =
    Bytes.add(vertices.map(_.innerId.bytes).foldLeft(Array.empty[Byte])(Bytes.add), cacheKeyBytes)

  lazy val fullCacheKey: Long = Hashing.murmur3_128().hashBytes(fullCacheKeyBytes).asLong()
}

object EdgeTransformer {
  val DefaultTransformField = Json.arr("_to")
  val DefaultTransformFieldAsList = Json.arr("_to").as[List[String]]
  val DefaultJson = Json.arr(DefaultTransformField)
}

/**
 * TODO: step wise outputFields should be used with nextStepLimit, nextStepThreshold.
 * @param jsValue
 */
case class EdgeTransformer(jsValue: JsValue) {
  val Delimiter = "\\$"
  val targets = jsValue.asOpt[List[Vector[String]]].toList
  val fieldsLs = for {
    target <- targets
    fields <- target
  } yield fields
  val isDefault = fieldsLs.size == 1 && fieldsLs.head.size == 1 && (fieldsLs.head.head == "_to" || fieldsLs.head.head == "to")

  def toHashKeyBytes: Array[Byte] = if (isDefault) Array.empty[Byte] else Bytes.toBytes(jsValue.toString)

  def replace(queryParam: QueryParam,
              fmt: String,
              values: Seq[InnerValLike],
              nextStepOpt: Option[Step]): Seq[InnerValLike] = {

    val tokens = fmt.split(Delimiter)
    val _values = values.padTo(tokens.length, InnerVal.withStr("", queryParam.label.schemaVersion))
    val mergedStr = tokens.zip(_values).map { case (prefix, innerVal) => prefix + innerVal.toString }.mkString
    //    logger.error(s"${tokens.toList}, ${values}, $mergedStr")
    //    println(s"${tokens.toList}, ${values}, $mergedStr")
    nextStepOpt match {
      case None =>
        val columnType =
          if (queryParam.labelWithDir.dir == GraphUtil.directions("out")) queryParam.label.tgtColumnType
          else queryParam.label.srcColumnType

        if (columnType == InnerVal.STRING) Seq(InnerVal.withStr(mergedStr, queryParam.label.schemaVersion))
        else Nil
      case Some(nextStep) =>
        val nextQueryParamsValid = nextStep.queryParams.filter { qParam =>
          if (qParam.labelWithDir.dir == GraphUtil.directions("out")) qParam.label.srcColumnType == "string"
          else qParam.label.tgtColumnType == "string"
        }
        for {
          nextQueryParam <- nextQueryParamsValid
        } yield {
          InnerVal.withStr(mergedStr, nextQueryParam.label.schemaVersion)
        }
    }
  }

  def toInnerValOpt(queryParam: QueryParam, edge: Edge, fieldName: String): Option[InnerValLike] = {
    fieldName match {
      case LabelMeta.to.name => Option(edge.tgtVertex.innerId)
      case LabelMeta.from.name => Option(edge.srcVertex.innerId)
      case _ =>
        for {
          labelMeta <- queryParam.label.metaPropsInvMap.get(fieldName)
          value <- edge.propsWithTs.get(labelMeta)
        } yield value.innerVal
    }
  }

  def transform(queryParam: QueryParam, edge: Edge, nextStepOpt: Option[Step]): Seq[Edge] = {
    if (isDefault) Seq(edge)
    else {
      val edges = for {
        fields <- fieldsLs
        innerVal <- {
          if (fields.size == 1) {
            val fieldName = fields.head
            toInnerValOpt(queryParam, edge, fieldName).toSeq
          } else {
            val fmt +: fieldNames = fields
            replace(queryParam, fmt, fieldNames.flatMap(fieldName => toInnerValOpt(queryParam, edge, fieldName)), nextStepOpt)
          }
        }
      } yield edge.updateTgtVertex(innerVal).copy(originalEdgeOpt = Option(edge))


      edges
    }
  }
}

object Step {
  val Delimiter = "|"
}

case class Step(queryParams: Seq[QueryParam],
                labelWeights: Map[Int, Double] = Map.empty,
                //                scoreThreshold: Double = 0.0,
                nextStepScoreThreshold: Double = 0.0,
                nextStepLimit: Int = -1,
                cacheTTL: Long = -1) {

//  lazy val excludes = queryParams.filter(_.exclude)
//  lazy val includes = queryParams.filterNot(_.exclude)
//  lazy val excludeIds = excludes.map(x => x.labelWithDir.labelId -> true).toMap

  lazy val cacheKeyBytes = queryParams.map(_.toCacheKeyRaw(Array.empty[Byte])).foldLeft(Array.empty[Byte])(Bytes.add)
  def toCacheKey(lss: Seq[Long]): Long = Hashing.murmur3_128().hashBytes(toCacheKeyRaw(lss)).asLong()
//    MurmurHash3.bytesHash(toCacheKeyRaw(lss))

  def toCacheKeyRaw(lss: Seq[Long]): Array[Byte] = {
    var bytes = Array.empty[Byte]
    lss.sorted.foreach { h => bytes = Bytes.add(bytes, Bytes.toBytes(h)) }
    bytes
  }
}

case class VertexParam(vertices: Seq[Vertex]) {
  var filters: Option[Map[Byte, InnerValLike]] = None

  def has(what: Option[Map[Byte, InnerValLike]]): VertexParam = {
    what match {
      case None => this
      case Some(w) => has(w)
    }
  }

  def has(what: Map[Byte, InnerValLike]): VertexParam = {
    this.filters = Some(what)
    this
  }

}

object RankParam {
  val Default = RankParam()
}

case class RankParam(keySeqAndWeights: Seq[(LabelMeta, Double)] = Seq((LabelMeta.count, 1.0))) {
  // empty => Count
  lazy val rankKeysWeightsMap = keySeqAndWeights.toMap

  def toHashKeyBytes(): Array[Byte] = {
    var bytes = Array.empty[Byte]
    keySeqAndWeights.map { case (labelMeta, weight) =>
      bytes = Bytes.add(bytes, Array.fill(1)(labelMeta.seq), Bytes.toBytes(weight))
    }
    bytes
  }
}
object QueryParam {
  lazy val Empty = QueryParam(labelName = "")
  lazy val DefaultThreshold = Double.MinValue
  val Delimiter = ","
  val maxMetaByte = (-1).toByte
  val fillArray = Array.fill(100)(maxMetaByte)

  def apply(labelWithDirection: LabelWithDirection): QueryParam = {
    val label = Label.findById(labelWithDirection.labelId)
    val direction = GraphUtil.fromDirection(labelWithDirection.dir)
    QueryParam(labelName = label.label, direction = direction)
  }
}
case class QueryParam(labelName: String,
                        direction: String = "out",
                        offset: Int = 0,
                        limit: Int = 100,
                        sample: Int = -1,
                        maxAttempt: Int = 2,
                        rpcTimeout: Int = 1000,
                        cacheTTLInMillis: Long = -1L,
                        indexName: String = LabelIndex.DefaultName,
                        where: Try[Where] = Success(WhereParser.success),
                        timestamp: Long = System.currentTimeMillis(),
                        threshold: Double = Double.MinValue,
                        rank: RankParam = RankParam.Default,
                        intervalOpt: Option[( (Seq[(String, Any)]), Seq[(String, Any)])] = None,
                        durationOpt: Option[(Long, Long)] = None,
                        exclude: Boolean = false,
                        include: Boolean = false,
                        has: Map[String, Any] = Map.empty,
                        duplicatePolicy: DuplicatePolicy = DuplicatePolicy.First,
                        includeDegree: Boolean = false,
                        scorePropagateShrinkage: Long = 500L,
                        scorePropagateOp: String = "multiply",
                        shouldNormalize: Boolean = false,
                        whereRawOpt: Option[String] = None,
                        cursorOpt: Option[String] = None,
                        tgtVertexIdOpt: Option[Any] = None,
                        edgeTransformer: EdgeTransformer = EdgeTransformer(EdgeTransformer.DefaultJson),
                        timeDecay: Option[TimeDecay] = None) {
  import JSONParser._
  //TODO: implement this.
  lazy val whereHasParent = true

  lazy val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))
  lazy val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"not supported direction: $direction"))

  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
  lazy val labelOrderSeq =
    if (indexName == LabelIndex.DefaultName) LabelIndex.DefaultSeq
    else label.indexNameMap.get(indexName).getOrElse(throw new RuntimeException(s"$indexName indexName is not found.")).seq
  lazy val tgtVertexInnerIdOpt = tgtVertexIdOpt.map { id =>
    val tmp = label.tgtColumnWithDir(dir)
    toInnerVal(id, tmp.columnType, tmp.schemaVersion)
  }
  lazy val (columnRangeFilter, columnRangeFilterMaxBytes, columnRangeFilterMinBytes)  = intervalOpt match {
    case None => (null, Array.empty[Byte], Array.empty[Byte])
    case Some(interval) =>
      val len = label.indicesMap(labelOrderSeq).sortKeyTypes.size.toByte
      val (minBytes, maxBytes) = paddingInterval(len, interval._1, interval._2)
      (new ColumnRangeFilter(minBytes, true, maxBytes, true), maxBytes, minBytes)
  }
  lazy val isSnapshotEdge = tgtVertexInnerIdOpt.isDefined

  /** since degree info is located on first always */
  lazy val (innerOffset, innerLimit) = if (columnRangeFilter == null) {
    if (offset == 0) (offset, limit + 1)
    else (offset + 1, limit)
  } else (offset, limit)

  def toBytes(idxSeq: Byte, offset: Int, limit: Int, isInverted: Boolean): Array[Byte] = {
    val front = Array[Byte](idxSeq, if (isInverted) 1.toByte else 0.toByte)
    Bytes.add(front, Bytes.toBytes((offset.toLong << 32 | limit)))
  }

  def toCacheKey(bytes: Array[Byte]): Long = {
    val hashBytes = toCacheKeyRaw(bytes)
    Hashing.murmur3_128().hashBytes(hashBytes).asLong()
  }

  def toCacheKeyRaw(bytes: Array[Byte]): Array[Byte] = {
    val transformBytes = edgeTransformer.toHashKeyBytes
    //TODO: change this to binrary format.
    val whereBytes = Bytes.toBytes(whereRawOpt.getOrElse(""))
    val durationBytes = durationOpt.map { case (min, max) =>
      val minTs = min / cacheTTLInMillis
      val maxTs = max / cacheTTLInMillis
      Bytes.add(Bytes.toBytes(minTs), Bytes.toBytes(maxTs))
    } getOrElse Array.empty[Byte]

    val conditionBytes = Bytes.add(transformBytes, whereBytes, durationBytes)
    val labelWithDirBytes = Bytes.add(Bytes.toBytes(label.id.get), Bytes.toBytes(direction))
    Bytes.add(Bytes.add(bytes, labelWithDirBytes, toBytes(labelOrderSeq, offset, limit, isSnapshotEdge)), rank.toHashKeyBytes(),
      Bytes.add(columnRangeFilterMinBytes, columnRangeFilterMaxBytes, conditionBytes))
  }

  private def convertToInner(kvs: Seq[(String, Any)]): Seq[(LabelMeta, InnerValLike)] = {
    kvs.map { kv =>
      val labelMeta = label.metaPropsInvMap.get(kv._1).getOrElse(throw new RuntimeException(s"$kv is not found in labelMetas."))
      labelMeta -> toInnerVal(kv._2, labelMeta.dataType, label.schemaVersion)
    }
  }
  private def paddingInterval(len: Byte, from: Seq[(String, Any)], to: Seq[(String, Any)]) = {
    val fromVal = Bytes.add(propsToBytes(convertToInner(from)), QueryParam.fillArray)
    val toVal = propsToBytes(convertToInner(to))

    toVal(0) = len
    fromVal(0) = len

    val minMax = (toVal, fromVal) // inverted
    minMax
  }

  def toLabelMetas(names: Seq[String]): Set[LabelMeta] = {
    val m = for {
      name <- names
      labelMeta <- label.metaPropsInvMap.get(name)
    } yield labelMeta
    m.toSet
  }
}

object DuplicatePolicy extends Enumeration {
  type DuplicatePolicy = Value
  val First, Sum, CountSum, Raw = Value

  def apply(policy: String): Value = {
    policy match {
      case "sum" => DuplicatePolicy.Sum
      case "countSum" => DuplicatePolicy.CountSum
      case "raw" => DuplicatePolicy.Raw
      case _ => DuplicatePolicy.First
    }
  }
}
case class TimeDecay(initial: Double = 1.0,
                     lambda: Double = 0.1,
                     timeUnit: Double = 60 * 60 * 24,
                     labelMeta: LabelMeta = LabelMeta.timestamp) {
  def decay(diff: Double): Double = {
    //FIXME
    val ret = initial * Math.pow(1.0 - lambda, diff / timeUnit)
    //    logger.debug(s"$initial, $lambda, $timeUnit, $diff, ${diff / timeUnit}, $ret")
    ret
  }
}
