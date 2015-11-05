package com.kakao.s2graph.core

import com.kakao.s2graph.core.OrderingUtil.{JsValueOrdering, MultiValueOrdering}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsNumber, JsValue}

import scala.util.Random

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 5..
 */
class OrderingUtilTest extends FunSuite with Matchers {
  val wrapStr = s"\n=================================================="

  def duration[T](prefix: String = "")(block: => T) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    println(s"$wrapStr\n$prefix: took ${endTs - startTs} ms$wrapStr")
    ret
  }

  test("test MultiOrdering") {
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
    val resultJsLs = jsLs.sorted(new MultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }

  test("performance MultiOrdering any") {
    val tupLs = (0 until 500) map { i =>
      Random.nextDouble() -> Random.nextLong()
    }

    val seqLs = tupLs.map { tup =>
      Seq(tup._1, tup._2)
    }

    duration("TupleOrdering any") {
      (0 until 10000) foreach { _ =>
        tupLs.sortBy { case (x, y) =>
          -x -> -y
        }
      }
    }

    duration("MultiOrdering any") {
      (0 until 10000) foreach { _ =>
        seqLs.sorted(new MultiOrdering[Any](Seq(false, false)))
      }
    }
  }

  test("performance MultiOrdering double") {
    val tupLs = (0 until 500) map { i =>
      Random.nextDouble() -> Random.nextDouble()
    }

    val seqLs = tupLs.map { tup =>
      Seq(tup._1, tup._2)
    }

    duration("MultiOrdering double") {
      (0 until 10000) foreach { _ =>
        seqLs.sorted(new MultiOrdering[Double](Seq(false, false)))
      }
    }

    duration("TupleOrdering double") {
      (0 until 10000) foreach { _ =>
        tupLs.sortBy { case (x, y) =>
          -x -> -y
        }
      }
    }
  }

  test("performance MultiOrdering jsvalue") {
    val tupLs = (0 until 500) map { i =>
      Random.nextDouble() -> Random.nextLong()
    }

    val seqLs = tupLs.map { tup =>
      Seq(JsNumber(tup._1), JsNumber(tup._2))
    }

    duration("TupleOrdering double,long") {
      (0 until 10000) foreach { _ =>
        tupLs.sortBy { case (x, y) =>
          -x -> -y
        }
      }
    }

    duration("MultiOrdering jsvalue") {
      (0 until 10000) foreach { _ =>
        seqLs.sorted(new MultiOrdering[JsValue](Seq(false, false)))
      }
    }
  }
}
