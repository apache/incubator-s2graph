package org.apache.s2graph.counter.util

class ReduceMapValue[T, U](op: (U, U) => U, default: U) {
   def apply(m1: Map[T, U], m2: Map[T, U]): Map[T, U] = {
     m1 ++ m2.map { case (k, v) =>
       k -> op(m1.getOrElse(k, default), v)
     }
   }
 }
