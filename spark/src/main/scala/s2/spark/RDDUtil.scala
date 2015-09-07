package s2.spark

import org.apache.spark.rdd.RDD

/**
 * Created by hsleep(honeysleep@gmail.com) on 14. 12. 23..
 */
object RDDUtil {
  def rddIsNonEmpty[T](rdd: RDD[T]): Boolean = {
    !rddIsEmpty(rdd)
  }

  def rddIsEmpty[T](rdd: RDD[T]): Boolean = {
    rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_ && _)
  }
}
