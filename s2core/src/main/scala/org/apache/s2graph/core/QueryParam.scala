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
import org.apache.s2graph.core.rest.TemplateHelper
import org.apache.s2graph.core.storage.serde.StorageSerializable._
import org.apache.s2graph.core.types._
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.hbase.async.ColumnRangeFilter
import play.api.libs.json.{JsNull, JsString, JsValue, Json}

import scala.util.{Success, Try}

object Query {
  val initialScore = 1.0
  lazy val empty = Query()

  def apply(query: Query): Query = {
    Query(query.vertices, query.steps, query.queryOption, query.jsonQuery)
  }

  def toQuery(srcVertices: Seq[S2VertexLike], queryParams: Seq[QueryParam]) = Query(srcVertices, Vector(Step(queryParams)))

}

case class MinShouldMatchParam(prop: String, count: Int, terms: Set[Any])

object GroupBy {
  val Empty = GroupBy()
}

case class GroupBy(keys: Seq[String] = Nil,
                   limit: Int = Int.MaxValue,
                   minShouldMatch: Option[MinShouldMatchParam] = None)

case class MultiQuery(queries: Seq[Query],
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
                       filterOutQuery: Option[Query] = None,
                       filterOutFields: Seq[String] = Seq(LabelMeta.to.name),
                       withScore: Boolean = true,
                       returnTree: Boolean = false,
                       limitOpt: Option[Int] = None,
                       returnAgg: Boolean = true,
                       scoreThreshold: Double = Double.MinValue,
                       returnDegree: Boolean = true,
                       impIdOpt: Option[String] = None,
                       shouldPropagateScore: Boolean = true,
                       ignorePrevStepCache: Boolean = false) {
  val orderByKeys = orderByColumns.map(_._1)
  val ascendingVals = orderByColumns.map(_._2)
  val selectColumnsMap = selectColumns.map { c => c -> true }.toMap
  val scoreFieldIdx = orderByKeys.zipWithIndex.find(t => t._1 == "score").map(_._2).getOrElse(-1)
  val (edgeSelectColumns, propsSelectColumns) = selectColumns.partition(c => LabelMeta.defaultRequiredMetaNames.contains(c))
  /** */
  val edgeSelectColumnsFiltered = edgeSelectColumns
  //  val edgeSelectColumnsFiltered = edgeSelectColumns.filterNot(c => groupBy.keys.contains(c))
  lazy val cacheKeyBytes: Array[Byte] = {
    val selectBytes = Bytes.toBytes(selectColumns.toString)
    val groupBytes = Bytes.toBytes(groupBy.keys.toString)
    val orderByBytes = Bytes.toBytes(orderByColumns.toString)
    val filterOutBytes = filterOutQuery.map(_.fullCacheBytes).getOrElse(Array.empty[Byte])
    val returnTreeBytes = Bytes.toBytes(returnTree)

    Seq(selectBytes, groupBytes, orderByBytes, filterOutBytes, returnTreeBytes).foldLeft(Array.empty[Byte])(Bytes.add)
  }

}

case class Query(vertices: Seq[S2VertexLike] = Nil,
                 steps: IndexedSeq[Step] = Vector.empty[Step],
                 queryOption: QueryOption = QueryOption(),
                 jsonQuery: JsValue = JsNull) {

  lazy val fullCacheBytes = {
    val srcBytes = vertices.map(_.innerId.bytes).foldLeft(Array.empty[Byte])(Bytes.add)
    val stepBytes = steps.map(_.cacheKeyBytes).foldLeft(Array.empty[Byte])(Bytes.add)
    val queryOptionBytes = queryOption.cacheKeyBytes
    Bytes.add(srcBytes, stepBytes, queryOptionBytes)
  }
  lazy val fullCacheKey: Long = Hashing.murmur3_128().hashBytes(fullCacheBytes).asLong()
}

object EdgeTransformer {
  val DefaultTransformField = Json.arr("_to")
  val DefaultTransformFieldAsList = Json.arr("_to").as[List[String]]
  val DefaultJson = Json.arr(DefaultTransformField)
}

/**
  * TODO: step wise outputFields should be used with nextStepLimit, nextStepThreshold.
  *
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

  def toInnerValOpt(queryParam: QueryParam, edge: S2EdgeLike, fieldName: String): Option[InnerValLike] = {
    fieldName match {
      case LabelMeta.to.name => Option(edge.tgtVertex.innerId)
      case LabelMeta.from.name => Option(edge.srcVertex.innerId)
      case _ => edge.propertyValue(fieldName).map(_.innerVal)
    }
  }

  def transform(queryParam: QueryParam, edge: S2EdgeLike, nextStepOpt: Option[Step]): Seq[S2EdgeLike] = {
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
      } yield edge.updateTgtVertex(innerVal).copyOriginalEdgeOpt(Option(edge))


      edges
    }
  }
}

object Step {
  val Delimiter = "|"
}

case class Step(queryParams: Seq[QueryParam],
                labelWeights: Map[Int, Double] = Map.empty,
                nextStepScoreThreshold: Double = 0.0,
                nextStepLimit: Int = -1,
                cacheTTL: Long = -1,
                groupBy: GroupBy = GroupBy.Empty) {

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

case class VertexParam(vertices: Seq[S2VertexLike]) {
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

  def score(edge: S2EdgeLike): Double = {
    if (keySeqAndWeights.size <= 0) 1.0f
    else {
      var sum: Double = 0

      for ((labelMeta, w) <- keySeqAndWeights) {
        if (edge.getPropsWithTs().containsKey(labelMeta.name)) {
          val innerValWithTs = edge.getPropsWithTs().get(labelMeta.name)
          val cost = try innerValWithTs.innerVal.toString.toDouble catch {
            case e: Exception => 1.0
          }
          sum += w * cost
        }
      }
      sum
    }
  }
}

object QueryParam {
  lazy val Empty = QueryParam(labelName = "")
  lazy val DefaultThreshold = Double.MinValue
  val Delimiter = ","
  val maxMetaByte = (-1).toByte
  val fillArray = Array.fill(100)(maxMetaByte)

  import scala.collection.JavaConverters._

  def apply(labelWithDirection: LabelWithDirection): QueryParam = {
    val label = Label.findById(labelWithDirection.labelId)
    val direction = GraphUtil.fromDirection(labelWithDirection.dir)
    QueryParam(labelName = label.label, direction = direction)
  }
}

object VertexQueryParam {
  def Empty: VertexQueryParam = VertexQueryParam(0, 1, None)
}

case class VertexQueryParam(offset: Int,
                            limit: Int,
                            searchString: Option[String],
                            vertexIds: Seq[VertexId] = Nil,
                            fetchProp: Boolean = true) {
}

case class QueryParam(labelName: String,
                      direction: String = "out",
                      offset: Int = 0,
                      limit: Int = S2Graph.DefaultFetchLimit,
                      sample: Int = -1,
                      maxAttempt: Int = 20,
                      rpcTimeout: Int = 600000,
                      cacheTTLInMillis: Long = -1L,
                      indexName: String = LabelIndex.DefaultName,
                      where: Try[Where] = Success(WhereParser.success),
                      timestamp: Long = System.currentTimeMillis(),
                      threshold: Double = Double.MinValue,
                      rank: RankParam = RankParam.Default,
                      intervalOpt: Option[((Seq[(String, JsValue)]), Seq[(String, JsValue)])] = None,
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

  lazy val label = Label.findByName(labelName).getOrElse(throw LabelNotExistException(labelName))
  lazy val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"not supported direction: $direction"))

  lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
  lazy val labelOrderSeq =
    if (indexName == LabelIndex.DefaultName) LabelIndex.DefaultSeq
    else label.indexNameMap.getOrElse(indexName, throw new RuntimeException(s"$indexName indexName is not found.")).seq

  lazy val tgtVertexInnerIdOpt = tgtVertexIdOpt.map { id =>
    CanInnerValLike.anyToInnerValLike.toInnerVal(id)(label.tgtColumnWithDir(dir).schemaVersion)
  }

  def buildInterval(edgeOpt: Option[S2EdgeLike]) = intervalOpt match {
    case None => Array.empty[Byte] -> Array.empty[Byte]
    case Some(interval) =>
      val (froms, tos) = interval

      val len = label.indicesMap(labelOrderSeq).sortKeyTypes.size.toByte
      val (maxBytes, minBytes) = paddingInterval(len, froms, tos, edgeOpt)

      maxBytes -> minBytes
  }

  lazy val isSnapshotEdge = tgtVertexInnerIdOpt.isDefined

  /** since degree info is located on first always */
  lazy val (innerOffset, innerLimit) = if (intervalOpt.isEmpty) {
    if (offset == 0) (offset, if (limit == Int.MaxValue) limit else limit + 1)
    else (offset + 1, limit)
  } else (offset, limit)

  lazy val optionalCacheKey: Array[Byte] = {
    val transformBytes = edgeTransformer.toHashKeyBytes
    //TODO: change this to binrary format.
    val whereBytes = Bytes.toBytes(whereRawOpt.getOrElse(""))
    val durationBytes = durationOpt.map { case (min, max) =>
      val minTs = min / cacheTTLInMillis
      val maxTs = max / cacheTTLInMillis
      Bytes.add(Bytes.toBytes(minTs), Bytes.toBytes(maxTs))
    } getOrElse Array.empty[Byte]

    val conditionBytes = Bytes.add(transformBytes, whereBytes, durationBytes)

    // Interval cache bytes is moved to fetch method
    Bytes.add(Bytes.add(toBytes(offset, limit), rank.toHashKeyBytes()), conditionBytes)
  }

  def toBytes(offset: Int, limit: Int): Array[Byte] = {
    Bytes.add(Bytes.toBytes(offset), Bytes.toBytes(limit))
  }

  def toCacheKey(bytes: Array[Byte]): Long = {
    val hashBytes = toCacheKeyRaw(bytes)
    Hashing.murmur3_128().hashBytes(hashBytes).asLong()
  }

  def toCacheKeyRaw(bytes: Array[Byte]): Array[Byte] = {
    Bytes.add(bytes, optionalCacheKey)
  }

  private def convertToInner(kvs: Seq[(String, JsValue)], edgeOpt: Option[S2EdgeLike]): Seq[(LabelMeta, InnerValLike)] = {
    kvs.map { case (propKey, propValJs) =>
      propValJs match {
        case JsString(in) if edgeOpt.isDefined && in.contains("_parent.") =>
          val parentLen = in.split("_parent.").length - 1
          val edge = (0 until parentLen).foldLeft(edgeOpt.get) { case (acc, _) => acc.getParentEdges().head.edge }

          val timePivot = edge.ts
          val replaced = TemplateHelper.replaceVariable(timePivot, in).trim

          val (_propKey, _padding) = replaced.span(ch => !ch.isDigit && ch != '-' && ch != '+' && ch != ' ')
          val propKey = _propKey.split("_parent.").last
          val padding = Try(_padding.trim.toLong).getOrElse(0L)

          val labelMeta = edge.innerLabel.metaPropsInvMap.getOrElse(propKey, throw new RuntimeException(s"$propKey not found in ${edge} labelMetas."))

          val propVal =
            if (InnerVal.isNumericType(labelMeta.dataType)) {
              InnerVal.withLong(edge.property(labelMeta.name).value.toString.toLong + padding, label.schemaVersion)
            } else {
              edge.property(labelMeta.name).asInstanceOf[S2Property[_]].innerVal
            }

          labelMeta -> propVal
        case _ =>
          val labelMeta = label.metaPropsInvMap.getOrElse(propKey, throw new RuntimeException(s"$propKey not found in labelMetas."))
          val propVal = jsValueToInnerVal(propValJs, labelMeta.dataType, label.schemaVersion)

          labelMeta -> propVal.get
      }
    }
  }

  def paddingInterval(len: Byte, froms: Seq[(String, JsValue)], tos: Seq[(String, JsValue)], edgeOpt: Option[S2EdgeLike] = None) = {
    val fromInnerVal = convertToInner(froms, edgeOpt)
    val toInnerVal = convertToInner(tos, edgeOpt)

    val fromVal = Bytes.add(propsToBytes(fromInnerVal), QueryParam.fillArray)
    val toVal = propsToBytes(toInnerVal)

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
