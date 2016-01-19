package com.kakao.s2graph.core

import com.kakao.s2graph.core.types.InnerValLike
import play.api.libs.json.{JsNumber, JsString, JsValue}

object OrderingUtil {

  implicit object JsValueOrdering extends Ordering[JsValue] {
    override def compare(x: JsValue, y: JsValue): Int = {
      (x, y) match {
        case (JsNumber(xv), JsNumber(yv)) =>
          Ordering.BigDecimal.compare(xv, yv)
        case (JsString(xv), JsString(yv)) =>
          Ordering.String.compare(xv, yv)
        case _ => throw new Exception(s"unsupported type")
      }
    }
  }

  implicit object InnerValLikeOrdering extends Ordering[InnerValLike] {
    override def compare(x: InnerValLike, y: InnerValLike): Int = {
      x.compare(y)
    }
  }

  implicit object MultiValueOrdering extends Ordering[Any] {
    override def compare(x: Any, y: Any): Int = {
      (x, y) match {
        case (xv: Int, yv: Int) => implicitly[Ordering[Int]].compare(xv, yv)
        case (xv: Long, yv: Long) => implicitly[Ordering[Long]].compare(xv, yv)
        case (xv: Double, yv: Double) => implicitly[Ordering[Double]].compare(xv, yv)
        case (xv: String, yv: String) => implicitly[Ordering[String]].compare(xv, yv)
        case (xv: JsValue, yv: JsValue) => implicitly[Ordering[JsValue]].compare(xv, yv)
        case (xv: InnerValLike, yv: InnerValLike) => implicitly[Ordering[InnerValLike]].compare(xv, yv)
      }
    }
  }

  def TupleMultiOrdering[T](ascendingLs: Seq[Boolean])(implicit ord: Ordering[T]): Ordering[(T, T, T, T)] = {
    new Ordering[(T, T, T, T)] {
      override def compare(tx: (T, T, T, T), ty: (T, T, T, T)): Int = {
        val len = ascendingLs.length
        val it = ascendingLs.iterator
        if (len >= 1) {
          val (x, y) = it.next() match {
            case true => tx -> ty
            case false => ty -> tx
          }
          val compare1 = ord.compare(x._1, y._1)
          if (compare1 != 0) return compare1
        }

        if (len >= 2) {
          val (x, y) = it.next() match {
            case true => tx -> ty
            case false => ty -> tx
          }
          val compare2 = ord.compare(x._2, y._2)
          if (compare2 != 0) return compare2
        }

        if (len >= 3) {
          val (x, y) = it.next() match {
            case true => tx -> ty
            case false => ty -> tx
          }
          val compare3 = ord.compare(x._3, y._3)
          if (compare3 != 0) return compare3
        }

        if (len >= 4) {
          val (x, y) = it.next() match {
            case true => tx -> ty
            case false => ty -> tx
          }
          val compare4 = ord.compare(x._4, y._4)
          if (compare4 != 0) return compare4
        }
        0
      }
    }
  }
}

class SeqMultiOrdering[T](ascendingLs: Seq[Boolean], defaultAscending: Boolean = true)(implicit ord: Ordering[T]) extends Ordering[Seq[T]] {
  override def compare(x: Seq[T], y: Seq[T]): Int = {
    val xe = x.iterator
    val ye = y.iterator
    val oe = ascendingLs.iterator

    while (xe.hasNext && ye.hasNext) {
      val ascending = if (oe.hasNext) oe.next() else defaultAscending
      val (xev, yev) = ascending match {
        case true => xe.next() -> ye.next()
        case false => ye.next() -> xe.next()
      }
      val res = ord.compare(xev, yev)
      if (res != 0) return res
    }

    Ordering.Boolean.compare(xe.hasNext, ye.hasNext)
  }
}

//class TupleMultiOrdering[T](ascendingLs: Seq[Boolean])(implicit ord: Ordering[T]) extends Ordering[(T, T, T, T)] {
//  override def compare(tx: (T, T, T, T), ty: (T, T, T, T)): Int = {
//    val len = ascendingLs.length
//    val it = ascendingLs.iterator
//    if (len >= 1) {
//      val (x, y) = it.next() match {
//        case true => tx -> ty
//        case false => ty -> tx
//      }
//      val compare1 = ord.compare(x._1, y._1)
//      if (compare1 != 0) return compare1
//    }
//
//    if (len >= 2) {
//      val (x, y) = it.next() match {
//        case true => tx -> ty
//        case false => ty -> tx
//      }
//      val compare2 = ord.compare(x._2, y._2)
//      if (compare2 != 0) return compare2
//    }
//
//    if (len >= 3) {
//      val (x, y) = it.next() match {
//        case true => tx -> ty
//        case false => ty -> tx
//      }
//      val compare3 = ord.compare(x._3, y._3)
//      if (compare3 != 0) return compare3
//    }
//
//    if (len >= 4) {
//      val (x, y) = it.next() match {
//        case true => tx -> ty
//        case false => ty -> tx
//      }
//      val compare4 = ord.compare(x._4, y._4)
//      if (compare4 != 0) return compare4
//    }
//    0
//  }
//}
