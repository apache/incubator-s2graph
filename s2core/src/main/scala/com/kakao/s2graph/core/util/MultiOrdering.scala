package com.kakao.s2graph.core.util

import play.api.libs.json.{JsString, JsNumber, JsValue}

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 4..
 */
class MultiOrdering(ascendingLs: Seq[Boolean], defaultAscending: Boolean = true) extends Ordering[Seq[JsValue]] {
  override def compare(x: Seq[JsValue], y: Seq[JsValue]): Int = {
    val xe = x.iterator
    val ye = y.iterator
    val oe = ascendingLs.iterator

    while (xe.hasNext && ye.hasNext) {
      val ascending = if (oe.hasNext) oe.next() else defaultAscending
      val (xev, yev) = ascending match {
        case true => xe.next() -> ye.next()
        case false => ye.next() -> xe.next()
      }
      val res = (xev, yev) match {
        case (JsNumber(xv), JsNumber(yv)) =>
          Ordering.BigDecimal.compare(xv, yv)
        case (JsString(xv), JsString(yv)) =>
          Ordering.String.compare(xv, yv)
        case _ => throw new Exception(s"unsupported type")
      }
      if (res != 0) return res
    }

    Ordering.Boolean.compare(xe.hasNext, ye.hasNext)
  }
}
