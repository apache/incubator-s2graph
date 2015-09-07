package org.apache.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 6..
 */
class KafkaRDDFunctions[T: ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  def foreachPartitionWithOffsetRange(f: (OffsetRange, Iterator[T]) => Unit): Unit = {
    val offsets = self.asInstanceOf[HasOffsetRanges].offsetRanges
    self.mapPartitionsWithIndex[Nothing] { case (i, part) =>
      val osr: OffsetRange = offsets(i)

      f(osr, part)

      Iterator.empty
    }.foreach {
      (_: Nothing) => ()
    }
  }
}

object KafkaRDDFunctions {
  implicit def rddToKafkaRDDFunctions[T: ClassTag](rdd: RDD[T]): KafkaRDDFunctions[T] = {
    new KafkaRDDFunctions(rdd)
  }
}
