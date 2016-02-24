package controllers

import com.kakao.s2graph.core.{OrderingUtil, SeqMultiOrdering}
import play.api.libs.json.{JsNumber, JsString, JsValue}
import play.api.test.PlaySpecification

class PostProcessSpec extends PlaySpecification {
  import OrderingUtil._

  "test order by json" >> {
    val jsLs: Seq[Seq[JsValue]] = Seq(
      Seq(JsNumber(0), JsString("a")),
      Seq(JsNumber(0), JsString("b")),
      Seq(JsNumber(1), JsString("a")),
      Seq(JsNumber(1), JsString("b")),
      Seq(JsNumber(2), JsString("c"))
    )

    // number descending, string ascending
    val sortedJsLs: Seq[Seq[JsValue]] = Seq(
      Seq(JsNumber(2), JsString("c")),
      Seq(JsNumber(1), JsString("a")),
      Seq(JsNumber(1), JsString("b")),
      Seq(JsNumber(0), JsString("a")),
      Seq(JsNumber(0), JsString("b"))
    )

    val orderParam: Seq[Boolean] = Seq(false, true)
    val resultJsLs = jsLs.sorted(new Ordering[Seq[JsValue]] {
      override def compare(x: Seq[JsValue], y: Seq[JsValue]): Int = {
        val xe = x.iterator
        val ye = y.iterator
        val oe = orderParam.iterator

        while (xe.hasNext && ye.hasNext && oe.hasNext) {
          val (xev, yev) = oe.next() match {
            case true => xe.next() -> ye.next()
            case false => ye.next() -> xe.next()
          }
          val res = (xev, yev) match {
            case (JsNumber(xv), JsNumber(yv)) =>
              Ordering[BigDecimal].compare(xv, yv)
            case (JsString(xv), JsString(yv)) =>
              Ordering[String].compare(xv, yv)
            case _ => throw new Exception("type mismatch")
          }
          if (res != 0) return res
        }

        Ordering.Boolean.compare(xe.hasNext, ye.hasNext)
      }
    })

    resultJsLs.toString() must_== sortedJsLs.toString
  }

  "test order by primitive type" >> {
    val jsLs: Seq[Seq[Any]] = Seq(
      Seq(0, "a"),
      Seq(0, "b"),
      Seq(1, "a"),
      Seq(1, "b"),
      Seq(2, "c")
    )

    // number descending, string ascending
    val sortedJsLs: Seq[Seq[Any]] = Seq(
      Seq(2, "c"),
      Seq(1, "a"),
      Seq(1, "b"),
      Seq(0, "a"),
      Seq(0, "b")
    )

    val ascendingLs: Seq[Boolean] = Seq(false, true)
    val resultJsLs = jsLs.sorted(new SeqMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() must_== sortedJsLs.toString
  }

  "test order by primitive type with short ascending list" >> {
    val jsLs: Seq[Seq[Any]] = Seq(
      Seq(0, "a"),
      Seq(1, "b"),
      Seq(0, "b"),
      Seq(1, "a"),
      Seq(2, "c"),
      Seq(1, "c"),
      Seq(1, "d"),
      Seq(1, "f"),
      Seq(1, "e")
    )

    // number descending, string ascending(default)
    val sortedJsLs: Seq[Seq[Any]] = Seq(
      Seq(2, "c"),
      Seq(1, "a"),
      Seq(1, "b"),
      Seq(1, "c"),
      Seq(1, "d"),
      Seq(1, "e"),
      Seq(1, "f"),
      Seq(0, "a"),
      Seq(0, "b")
    )

    val ascendingLs: Seq[Boolean] = Seq(false)
    val resultJsLs = jsLs.sorted(new SeqMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() must_== sortedJsLs.toString
  }
}
