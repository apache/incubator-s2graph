package org.apache.s2graph.core

import org.apache.s2graph.core.S2Graph.{FilterHashKey, HashKey}
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection, VertexId}
import org.apache.s2graph.core.utils.logger

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.Future
import scala.util.Random

object TraversalHelper {
  @tailrec
  final def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
    if (range < sampleNumber || set.size == sampleNumber) set
    else randomInt(sampleNumber, range, set + Random.nextInt(range))
  }

  def sample(queryRequest: QueryRequest, edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
    if (edges.size <= n) {
      edges
    } else {
      val plainEdges = if (queryRequest.queryParam.offset == 0) {
        edges.tail
      } else edges

      val randoms = randomInt(n, plainEdges.size)
      var samples = List.empty[EdgeWithScore]
      var idx = 0
      plainEdges.foreach { e =>
        if (randoms.contains(idx)) samples = e :: samples
        idx += 1
      }
      samples
    }
  }

  def normalize(edgeWithScores: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    val sum = edgeWithScores.foldLeft(0.0) { case (acc, cur) => acc + cur.score }
    edgeWithScores.map { edgeWithScore =>
      edgeWithScore.copy(score = edgeWithScore.score / sum)
    }
  }

  def alreadyVisitedVertices(edgeWithScoreLs: Seq[EdgeWithScore]): Map[(Int, Int, S2VertexLike), Boolean] = {
    val vertices = for {
      edgeWithScore <- edgeWithScoreLs
      edge = edgeWithScore.edge
      vertex = if (edge.getDir() == GraphUtil.directions("out")) edge.tgtVertex else edge.srcVertex
    } yield (edge.getLabelId(), edge.getDir(), vertex) -> true

    vertices.toMap
  }

  /** common methods for filter out, transform, aggregate queryResult */
  def convertEdges(queryParam: QueryParam, edge: S2EdgeLike, nextStepOpt: Option[Step]): Seq[S2EdgeLike] = {
    for {
      convertedEdge <- queryParam.edgeTransformer.transform(queryParam, edge, nextStepOpt) if !edge.isDegree
    } yield convertedEdge
  }

  def processTimeDecay(queryParam: QueryParam, edge: S2EdgeLike) = {
    /* process time decay */
    val tsVal = queryParam.timeDecay match {
      case None => 1.0
      case Some(timeDecay) =>
        val tsVal = try {
          val innerValWithTsOpt = edge.propertyValue(timeDecay.labelMeta.name)
          innerValWithTsOpt.map { innerValWithTs =>
            val innerVal = innerValWithTs.innerVal
            timeDecay.labelMeta.dataType match {
              case InnerVal.LONG => innerVal.value match {
                case n: BigDecimal => n.bigDecimal.longValue()
                case _ => innerVal.toString().toLong
              }
              case _ => innerVal.toString().toLong
            }
          } getOrElse (edge.ts)
        } catch {
          case e: Exception =>
            logger.error(s"processTimeDecay error. ${edge.toLogString}", e)
            edge.ts
        }
        val timeDiff = queryParam.timestamp - tsVal
        timeDecay.decay(timeDiff)
    }

    tsVal
  }

  def processDuplicates[R](queryParam: QueryParam,
                           duplicates: Seq[(FilterHashKey, R)])(implicit ev: WithScore[R]): Seq[(FilterHashKey, R)] = {

    if (queryParam.label.consistencyLevel != "strong") {
      //TODO:
      queryParam.duplicatePolicy match {
        case DuplicatePolicy.First => Seq(duplicates.head)
        case DuplicatePolicy.Raw => duplicates
        case DuplicatePolicy.CountSum =>
          val countSum = duplicates.size
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, countSum))
        case _ =>
          val scoreSum = duplicates.foldLeft(0.0) { case (prev, current) => prev + ev.score(current._2) }
          val (headFilterHashKey, headEdgeWithScore) = duplicates.head
          Seq(headFilterHashKey -> ev.withNewScore(headEdgeWithScore, scoreSum))
      }
    } else {
      duplicates
    }
  }

  def toHashKey(queryParam: QueryParam, edge: S2EdgeLike, isDegree: Boolean): (HashKey, FilterHashKey) = {
    val src = edge.srcVertex.innerId.hashCode()
    val tgt = edge.tgtVertex.innerId.hashCode()
    val hashKey = (src, edge.getLabelId(), edge.getDir(), tgt, isDegree)
    val filterHashKey = (src, tgt)

    (hashKey, filterHashKey)
  }
}


class TraversalHelper(graph: S2GraphLike) {
  import TraversalHelper._

  def buildNextStepQueryRequests(orgQuery: Query, stepIdx: Int, stepInnerResult: StepResult) = {
    val edgeWithScoreLs = stepInnerResult.edgeWithScores

    val q = orgQuery
    val queryOption = orgQuery.queryOption
    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)

    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(Int, Int, S2VertexLike), Boolean]
      else alreadyVisitedVertices(stepInnerResult.edgeWithScores)

    val initial = (Map.empty[S2VertexLike, Double], Map.empty[S2VertexLike, ArrayBuffer[EdgeWithScore]])
    val (sums, grouped) = edgeWithScoreLs.foldLeft(initial) { case ((sum, group), edgeWithScore) =>
      val key = edgeWithScore.edge.tgtVertex
      val newScore = sum.getOrElse(key, 0.0) + edgeWithScore.score
      val buffer = group.getOrElse(key, ArrayBuffer.empty[EdgeWithScore])
      buffer += edgeWithScore
      (sum + (key -> newScore), group + (key -> buffer))
    }
    val groupedByFiltered = sums.filter(t => t._2 >= prevStepThreshold)
    val prevStepTgtVertexIdEdges = grouped.map(t => t._1.id -> t._2)

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }

    val queryRequests = for {
      (vertex, prevStepScore) <- nextStepSrcVertices
      queryParam <- step.queryParams
    } yield {
      val labelWeight = step.labelWeights.getOrElse(queryParam.labelWithDir.labelId, 1.0)
      val newPrevStepScore = if (queryOption.shouldPropagateScore) prevStepScore else 1.0
      QueryRequest(q, stepIdx, vertex, queryParam, newPrevStepScore, labelWeight)
    }
    (alreadyVisited, prevStepTgtVertexIdEdges, queryRequests)
  }

  def filterEdges(q: Query,
                  stepIdx: Int,
                  queryRequests: Seq[QueryRequest],
                  queryResultLsFuture: Future[Seq[StepResult]],
                  queryParams: Seq[QueryParam],
                  buildLastStepInnerResult: Boolean = true,
                  parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                 (implicit ec: scala.concurrent.ExecutionContext): Future[StepResult] = {

    queryResultLsFuture.map { queryRequestWithResultLs =>
      val (cursors, failCount) = {
        val _cursors = ArrayBuffer.empty[Array[Byte]]
        var _failCount = 0

        queryRequestWithResultLs.foreach { stepResult =>
          _cursors.append(stepResult.cursors: _*)
          _failCount += stepResult.failCount
        }

        _cursors -> _failCount
      }


      if (queryRequestWithResultLs.isEmpty) StepResult.Empty.copy(failCount = failCount)
      else {
        val isLastStep = stepIdx == q.steps.size - 1
        val queryOption = q.queryOption
        val step = q.steps(stepIdx)

        val currentStepResults = queryRequests.view.zip(queryRequestWithResultLs)
        val shouldBuildInnerResults = !isLastStep || buildLastStepInnerResult
        val degrees = queryRequestWithResultLs.flatMap(_.degreeEdges)

        if (shouldBuildInnerResults) {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            edgeWithScore
          }

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)
          StepResult(edgeWithScores = results, grouped = Nil, degreeEdges = degrees, cursors = cursors, failCount = failCount)

        } else {
          val _results = buildResult(q, stepIdx, currentStepResults, parentEdges) { (edgeWithScore, propsSelectColumns) =>
            val edge = edgeWithScore.edge
            val score = edgeWithScore.score

            /* Select */
            val mergedPropsWithTs = edge.propertyValuesInner(propsSelectColumns)

            //            val newEdge = edge.copy(propsWithTs = mergedPropsWithTs)
            val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)

            val newEdgeWithScore = edgeWithScore.copy(edge = newEdge)
            /* OrderBy */
            val orderByValues =
              if (queryOption.orderByKeys.isEmpty) (score, edge.getTsInnerValValue(), None, None)
              else StepResult.toTuple4(newEdgeWithScore.toValues(queryOption.orderByKeys))

            /* StepGroupBy */
            val stepGroupByValues = newEdgeWithScore.toValues(step.groupBy.keys)

            /* GroupBy */
            val groupByValues = newEdgeWithScore.toValues(queryOption.groupBy.keys)

            /* FilterOut */
            val filterOutValues = newEdgeWithScore.toValues(queryOption.filterOutFields)

            newEdgeWithScore.copy(orderByValues = orderByValues,
              stepGroupByValues = stepGroupByValues,
              groupByValues = groupByValues,
              filterOutValues = filterOutValues)
          }

          /* process step group by */
          val results = StepResult.filterOutStepGroupBy(_results, step.groupBy)

          /* process ordered list */
          val ordered = if (queryOption.groupBy.keys.isEmpty) StepResult.orderBy(queryOption, results) else Nil

          /* process grouped list */
          val grouped =
            if (queryOption.groupBy.keys.isEmpty) Nil
            else {
              val agg = new scala.collection.mutable.HashMap[StepResult.GroupByKey, (Double, StepResult.Values)]()
              results.groupBy { edgeWithScore =>
                //                edgeWithScore.groupByValues.map(_.map(_.toString))
                edgeWithScore.groupByValues
              }.foreach { case (k, ls) =>
                val (scoreSum, merged) = StepResult.mergeOrdered(ls, Nil, queryOption)

                val newScoreSum = scoreSum

                /*
                  * watch out here. by calling toString on Any, we lose type information which will be used
                  * later for toJson.
                  */
                if (merged.nonEmpty) {
                  val newKey = merged.head.groupByValues
                  agg += ((newKey, (newScoreSum, merged)))
                }
              }
              agg.toSeq.sortBy(_._2._1 * -1)
            }

          StepResult(edgeWithScores = ordered, grouped = grouped, degreeEdges = degrees, cursors = cursors, failCount = failCount)
        }
      }
    }
  }




  private def toEdgeWithScores(queryRequest: QueryRequest,
                       stepResult: StepResult,
                       parentEdges: Map[VertexId, Seq[EdgeWithScore]]): Seq[EdgeWithScore] = {
    val queryOption = queryRequest.query.queryOption
    val queryParam = queryRequest.queryParam
    val prevScore = queryRequest.prevStepScore
    val labelWeight = queryRequest.labelWeight
    val edgeWithScores = stepResult.edgeWithScores

    val shouldBuildParents = queryOption.returnTree || queryParam.whereHasParent
    val parents = if (shouldBuildParents) {
      parentEdges.getOrElse(queryRequest.vertex.id, Nil).map { edgeWithScore =>
        val edge = edgeWithScore.edge

        /* Select */
        val mergedPropsWithTs =
          if (queryOption.selectColumns.isEmpty) {
            edge.propertyValuesInner()
          } else {
            val initial = Map(LabelMeta.timestamp -> edge.propertyValueInner(LabelMeta.timestamp))
            edge.propertyValues(queryOption.selectColumns) ++ initial
          }

        val newEdge = edge.copyEdgeWithState(mergedPropsWithTs)
        edgeWithScore.copy(edge = newEdge)
      }
    } else Nil

    // skip
    if (queryOption.ignorePrevStepCache) stepResult.edgeWithScores
    else {
      val degreeScore = 0.0

      val sampled =
        if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        else edgeWithScores

      val withScores = for {
        edgeWithScore <- sampled
      } yield {
        val edge = edgeWithScore.edge
        val edgeScore = edgeWithScore.score
        val score = queryParam.scorePropagateOp match {
          case "plus" => edgeScore + prevScore
          case "divide" =>
            if ((prevScore + queryParam.scorePropagateShrinkage) == 0) 0
            else edgeScore / (prevScore + queryParam.scorePropagateShrinkage)
          case _ => edgeScore * prevScore
        }

        val tsVal = processTimeDecay(queryParam, edge)
        val newScore = degreeScore + score
        //          val newEdge = if (queryOption.returnTree) edge.copy(parentEdges = parents) else edge
        val newEdge = edge.copyParentEdges(parents)
        edgeWithScore.copy(edge = newEdge, score = newScore * labelWeight * tsVal)
      }

      val normalized =
        if (queryParam.shouldNormalize) normalize(withScores)
        else withScores

      normalized
    }
  }

  private def buildResult[R](query: Query,
                     stepIdx: Int,
                     stepResultLs: Seq[(QueryRequest, StepResult)],
                     parentEdges: Map[VertexId, Seq[EdgeWithScore]])
                    (createFunc: (EdgeWithScore, Seq[LabelMeta]) => R)
                    (implicit ev: WithScore[R]): ListBuffer[R] = {
    import scala.collection._

    val results = ListBuffer.empty[R]
    val sequentialLs: ListBuffer[(HashKey, FilterHashKey, R, QueryParam)] = ListBuffer.empty
    val duplicates: mutable.HashMap[HashKey, ListBuffer[(FilterHashKey, R)]] = mutable.HashMap.empty
    val edgesToExclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty
    val edgesToInclude: mutable.HashSet[FilterHashKey] = mutable.HashSet.empty

    var numOfDuplicates = 0
    val queryOption = query.queryOption
    val step = query.steps(stepIdx)
    val excludeLabelWithDirSet = step.queryParams.filter(_.exclude).map(l => l.labelWithDir).toSet
    val includeLabelWithDirSet = step.queryParams.filter(_.include).map(l => l.labelWithDir).toSet

    stepResultLs.foreach { case (queryRequest, stepInnerResult) =>
      val queryParam = queryRequest.queryParam
      val label = queryParam.label
      val shouldBeExcluded = excludeLabelWithDirSet.contains(queryParam.labelWithDir)
      val shouldBeIncluded = includeLabelWithDirSet.contains(queryParam.labelWithDir)

      val propsSelectColumns = (for {
        column <- queryOption.propsSelectColumns
        labelMeta <- label.metaPropsInvMap.get(column)
      } yield labelMeta)

      for {
        edgeWithScore <- toEdgeWithScores(queryRequest, stepInnerResult, parentEdges)
      } {
        val edge = edgeWithScore.edge
        val (hashKey, filterHashKey) = toHashKey(queryParam, edge, isDegree = false)
        //        params += (hashKey -> queryParam) //

        /* check if this edge should be exlcuded. */
        if (shouldBeExcluded) {
          edgesToExclude.add(filterHashKey)
        } else {
          if (shouldBeIncluded) {
            edgesToInclude.add(filterHashKey)
          }
          val newEdgeWithScore = createFunc(edgeWithScore, propsSelectColumns)

          sequentialLs += ((hashKey, filterHashKey, newEdgeWithScore, queryParam))
          duplicates.get(hashKey) match {
            case None =>
              val newLs = ListBuffer.empty[(FilterHashKey, R)]
              newLs += (filterHashKey -> newEdgeWithScore)
              duplicates += (hashKey -> newLs) //
            case Some(old) =>
              numOfDuplicates += 1
              old += (filterHashKey -> newEdgeWithScore) //
          }
        }
      }
    }


    if (numOfDuplicates == 0) {
      // no duplicates at all.
      for {
        (hashKey, filterHashKey, edgeWithScore, _) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
      } {
        results += edgeWithScore
      }
    } else {
      // need to resolve duplicates.
      val seen = new mutable.HashSet[HashKey]()
      for {
        (hashKey, filterHashKey, edgeWithScore, queryParam) <- sequentialLs
        if !edgesToExclude.contains(filterHashKey) || edgesToInclude.contains(filterHashKey)
        if !seen.contains(hashKey)
      } {
        //        val queryParam = params(hashKey)
        processDuplicates(queryParam, duplicates(hashKey)).foreach { case (_, duplicate) =>
          if (ev.score(duplicate) >= queryParam.threshold) {
            seen += hashKey
            results += duplicate
          }
        }
      }
    }
    results
  }
}
