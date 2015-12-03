package com.kakao.ml.util

import org.scalatest.{Matchers, FunSuite}

class AsciiTableTest extends FunSuite with Matchers {

  val rows = Seq(
    Seq("s2graph", "is", "a", "graph", "database"),
    Seq("s2graph", "is", "a", "graph", "database")
  )
  val header = Seq("a", "b", "c", "d", "e")

  test("working") {
    AsciiTable(rows).show()
    AsciiTable(rows, header).show()
  }

}
