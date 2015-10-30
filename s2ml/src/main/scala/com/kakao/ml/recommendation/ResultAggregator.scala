package com.kakao.ml.recommendation

import com.kakao.ml.util.AsciiTable

case class ResultAggregator(
    numFeedbacks: Long,
    numRecommendations: Long,
    hits: Map[String, Long],
    averagePrecision: Double,
    num: Long) {

  def +(other: ResultAggregator) = {
    val newHits = hits.flatMap { case (key, value) => other.hits.get(key).map(v => key -> (v + value)) }
    ResultAggregator(
      numFeedbacks + other.numFeedbacks,
      numRecommendations + other.numRecommendations,
      newHits,
      averagePrecision + other.averagePrecision,
      num + other.num)
  }

  def precision: Map[String, Double] = hits.map { case (key, value) => key -> (value.toDouble / numRecommendations) }

  def recall: Map[String, Double] = hits.map { case (key, value) => key -> (value.toDouble / numFeedbacks) }

  /**
   * https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision
   */
  def meanAveragePrecision: Double = averagePrecision / num

  def getHeader: Seq[String] = Seq("numFeedbacks", "numRecommendations", "type", "hits", "precision", "recall", "meanAveragePrecision")

  def getRows: Seq[Seq[Any]] = {
    val rows = hits.toSeq.sortBy(_._1).map { case (key, value) =>
      Seq(numFeedbacks,
        numRecommendations,
        key,
        value,
        f"${100 * value.toDouble / numRecommendations}%.2f%%",
        f"${100 * value.toDouble / numFeedbacks}%.2f%%",
        f"${100 * meanAveragePrecision}%.2f%%")
    }
    rows
  }

  override def toString: String = {
    val header = getHeader
    val rows = getRows
    AsciiTable(rows, header).showString(100, false)
  }

}
