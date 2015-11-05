package com.kakao.s2graph.core

import com.kakao.s2graph.core.types.InnerValLike
import play.api.libs.json.{JsValue, JsNumber, JsString}

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 5..
 */
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
}
