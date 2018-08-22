package org.apache.s2graph.s2jobs.wal.udfs

import com.google.common.hash.Hashing
import org.apache.s2graph.core.JSONParser
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import play.api.libs.json._

import scala.reflect.ClassTag

object WalLogUDF {

  import scala.collection.mutable

  type MergedProps = Map[String, Seq[String]]
  type MutableMergedProps = mutable.Map[String, mutable.Map[String, Int]]
  type MutableMergedPropsInner = mutable.Map[String, Int]

  def initMutableMergedPropsInner = mutable.Map.empty[String, Int]

  def initMutableMergedProps = mutable.Map.empty[String, mutable.Map[String, Int]]

//  //TODO:
//  def toDimension(rawActivity: RawActivity, propertyKey: String): String = {
//    //    val (ts, dst, label, _) = rawActivity
//    //    label + "." + propertyKey
//    propertyKey
//  }
//
//  def updateMutableMergedProps(mutableMergedProps: MutableMergedProps)(dimension: String,
//                                                                       dimensionValue: String,
//                                                                       count: Int = 1): Unit = {
//    val buffer = mutableMergedProps.getOrElseUpdate(dimension, initMutableMergedPropsInner)
//    val newCount = buffer.getOrElse(dimensionValue, 0) + count
//    buffer += (dimensionValue -> newCount)
//  }
//
//  def groupByDimensionValues(rawActivity: RawActivity,
//                             propsJson: JsObject,
//                             mergedProps: MutableMergedProps,
//                             toDimensionFunc: (RawActivity, String) => String,
//                             excludePropKeys: Set[String] = Set.empty): Unit = {
//    propsJson.fields.filter(t => !excludePropKeys(t._1)).foreach { case (propertyKey, jsValue) =>
//      val values = jsValue match {
//        case JsString(s) => Seq(s)
//        case JsArray(arr) => arr.map(JSONParser.jsValueToString)
//        case _ => Seq(jsValue.toString())
//      }
//      val dimension = toDimensionFunc(rawActivity, propertyKey)
//
//      values.foreach { value =>
//        updateMutableMergedProps(mergedProps)(dimension, value)
//      }
//    }
//  }
//
//  def buildMergedProps(rawActivities: Seq[RawActivity],
//                       toDimensionFunc: (RawActivity, String) => String,
//                       defaultTopKs: Int = 100,
//                       dimTopKs: Map[String, Int] = Map.empty,
//                       excludePropKeys: Set[String] = Set.empty,
//                       dimValExtractors: Seq[Extractor] = Nil): MergedProps = {
//    val mergedProps = initMutableMergedProps
//
//    rawActivities.foreach { case rawActivity@(_, _, _, rawProps) =>
//      val propsJson = Json.parse(rawProps).as[JsObject]
//      groupByDimensionValues(rawActivity, propsJson, mergedProps, toDimensionFunc, excludePropKeys)
//    }
//    // work on extra dimVals.
//    dimValExtractors.foreach { extractor =>
//      extractor.extract(rawActivities, mergedProps)
//    }
//
//    mergedProps.map { case (key, values) =>
//      val topK = dimTopKs.getOrElse(key, defaultTopKs)
//
//      key -> values.toSeq.sortBy(-_._2).take(topK).map(_._1)
//    }.toMap
//  }
//
//  def rowToRawActivity(row: Row): RawActivity = {
//    (row.getAs[Long](0), row.getAs[String](1), row.getAs[String](2), row.getAs[String](3))
//  }
//
//  def appendMergeProps(toDimensionFunc: (RawActivity, String) => String = toDimension,
//                       defaultTopKs: Int = 100,
//                       dimTopKs: Map[String, Int] = Map.empty,
//                       excludePropKeys: Set[String] = Set.empty,
//                       dimValExtractors: Seq[Extractor] = Nil,
//                       minTs: Long = 0,
//                       maxTs: Long = Long.MaxValue) = udf((acts: Seq[Row]) => {
//    val rows = acts.map(rowToRawActivity).filter(act => act._1 >= minTs && act._1 < maxTs)
//
//    buildMergedProps(rows, toDimensionFunc, defaultTopKs, dimTopKs, excludePropKeys, dimValExtractors)
//  })

  val extractDimensionValues = {
    udf((dimensionValues: Map[String, Seq[String]]) => {
      dimensionValues.toSeq.flatMap { case (dimension, values) =>
        values.map { value => dimension -> value }
      }
    })
  }

  def toHash(dimension: String, dimensionValue: String): Long = {
    val key = s"$dimension.$dimensionValue"
    Hashing.murmur3_128().hashBytes(key.toString.getBytes("UTF-8")).asLong()
  }

  def filterDimensionValues(validDimValues: Broadcast[Set[Long]]) = {
    udf((dimensionValues: Map[String, Seq[String]]) => {
      dimensionValues.map { case (dimension, values) =>
        val filtered = values.filter { value =>
          val hash = toHash(dimension, value)

          validDimValues.value(hash)
        }

        dimension -> filtered
      }
    })
  }

  def appendRank[K1: ClassTag, K2: ClassTag, V: ClassTag](ds: Dataset[((K1, K2), V)],
                                                          numOfPartitions: Option[Int] = None,
                                                          samplePointsPerPartitionHint: Option[Int] = None)(implicit ordering: Ordering[(K1, K2)]) = {
    import org.apache.spark.RangePartitioner
    val rdd = ds.rdd

    val partitioner = new RangePartitioner(numOfPartitions.getOrElse(rdd.partitions.size),
      rdd,
      true,
      samplePointsPerPartitionHint = samplePointsPerPartitionHint.getOrElse(20)
    )

    val sorted = rdd.repartitionAndSortWithinPartitions(partitioner)

    def rank(idx: Int, iter: Iterator[((K1, K2), V)]) = {
      var curOffset = 1L
      var curK1 = null.asInstanceOf[K1]

      iter.map{ case ((key1, key2), value) =>
        //        println(s">>>[$idx] curK1: $curK1, curOffset: $curOffset")
        val newOffset = if (curK1 == key1) curOffset + 1L  else 1L
        curOffset = newOffset
        curK1 = key1
        (idx, newOffset, key1, key2, value)
      }
    }

    def getOffset(idx: Int, iter: Iterator[((K1, K2), V)]) = {
      val buffer = mutable.Map.empty[K1, (Int, Long)]
      if (!iter.hasNext) buffer.toIterator
      else {
        val ((k1, k2), v) = iter.next()
        var prevKey1: K1 = k1
        var size = 1L
        iter.foreach { case ((k1, k2), v) =>
          if (prevKey1 != k1) {
            buffer += prevKey1 -> (idx, size)
            prevKey1 = k1
            size = 0L
          }
          size += 1L
        }
        if (size > 0) buffer += prevKey1 -> (idx, size)
        buffer.iterator
      }
    }

    val partRanks = sorted.mapPartitionsWithIndex(rank)
    val _offsets = sorted.mapPartitionsWithIndex(getOffset)
    val offsets = _offsets.groupBy(_._1).flatMap { case (k1, partitionWithSize) =>
      val ls = partitionWithSize.toSeq.map(_._2).sortBy(_._1)
      var sum = ls.head._2
      val lss = ls.tail.map { case (partition, size) =>
        val x = (partition, sum)
        sum += size
        x
      }
      lss.map { case (partition, offset) =>
        (k1, partition) -> offset
      }
    }.collect()

    println(offsets)

    val offsetsBCast = ds.sparkSession.sparkContext.broadcast(offsets)

    def adjust(iter: Iterator[(Int, Long, K1, K2, V)], startOffsets: Map[(K1, Int), Long]) = {
      iter.map { case (partition, rankInPartition, key1, key2, value) =>
        val startOffset = startOffsets.getOrElse((key1, partition), 0L)
        val rank = startOffset + rankInPartition

        (partition, rankInPartition, rank, (key1, key2), value)
      }
    }

    val withRanks = partRanks
      .mapPartitions { iter =>
        val startOffsets = offsetsBCast.value.toMap
        adjust(iter, startOffsets)
      }.map { case (_, _, rank, (key1, key2), value) =>
      (rank, (key1, key2), value)
    }

    withRanks
  }
}
