package org.apache.spark.mllib.linalg

object Linalg {

  def scal(a: Double, s: Vector) = BLAS.scal(a, s)

  def dot(x: Vector, y: Vector) = BLAS.dot(x, y)

  def foreachActive(m: Matrix)(func: (Int, Int, Double) => Unit) = {
    m.foreachActive { (i, j, r) =>
      func(i, j, r)
    }
  }

}
