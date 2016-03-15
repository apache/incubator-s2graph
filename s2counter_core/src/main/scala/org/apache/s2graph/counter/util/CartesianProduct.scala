package org.apache.s2graph.counter.util

object CartesianProduct {
  def apply[T](xss: List[List[T]]): List[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for(xh <- h; xt <- apply(t)) yield xh :: xt
  }
}
