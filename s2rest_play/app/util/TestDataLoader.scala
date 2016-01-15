package util

import java.io.File

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.io.Source
import scala.util.Random


object TestDataLoader {
  val step = 100
  val prob = 1.0
  val (testIds, testIdsHist, testIdsHistCnt) = loadSeeds("./talk_vertices.txt")
  val maxId = testIds.length
  //  val randoms = (0 until 100).map{ i => new SecureRandom }
  //  val idx = new AtomicInteger(0)
  //  def randomId() = {
  //    val r = randoms(idx.getAndIncrement() % randoms.size)
  //    testAccountIds(r.nextInt(maxId))
  //  }
  def randomId(histStep: Int) = {
    for {
      maxId <- testIdsHistCnt.get(histStep)
      rIdx = Random.nextInt(maxId.toInt)
      hist <- testIdsHist.get(histStep)
      id = hist(rIdx)
    } yield {
//      logger.debug(s"randomId: $histStep = $id[$rIdx / $maxId]")
      id
    }
  }
  def randomId() = {
    val id = testIds(Random.nextInt(maxId))
    //    logger.debug(s"$id")
    id
  }
  private def loadSeeds(filePath: String) = {
    val histogram = new HashMap[Long, ListBuffer[Long]]
    val histogramCnt = new HashMap[Long, Long]
    val ids = new ArrayBuffer[Long]

    var idx = 0
//    logger.debug(s"$filePath start to load file.")
    for (line <- Source.fromFile(new File(filePath)).getLines) {
      //      testAccountIds(idx) = line.toLong
//      if (idx % 10000 == 0) logger.debug(s"$idx")
      idx += 1

      val parts = line.split("\\t")
      val id = parts.head.toLong
      val count = parts.last.toLong / step
      if (count > 1 && Random.nextDouble < prob) {
        histogram.get(count) match {
          case None =>
            histogram.put(count, new ListBuffer[Long])
            histogram.get(count).get += id
            histogramCnt.put(count, 1)
          case Some(existed) =>
            existed += id
            histogramCnt.put(count, histogramCnt.getOrElse(count, 0L) + 1L)
        }
        ids += id
      }

    }
//    logger.debug(s"upload $filePath finished.")
//    logger.debug(s"${histogram.size}")
    (ids, histogram.map(t => (t._1 -> t._2.toArray[Long])), histogramCnt)
  }
}
