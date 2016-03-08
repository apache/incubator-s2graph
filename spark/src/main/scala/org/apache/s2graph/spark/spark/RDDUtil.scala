package org.apache.s2graph.spark.spark

import org.apache.spark.rdd.RDD

object RDDUtil {
  def rddIsNonEmpty[T](rdd: RDD[T]): Boolean = {
    !rddIsEmpty(rdd)
  }

  def rddIsEmpty[T](rdd: RDD[T]): Boolean = {
    rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_ && _)
  }
}
