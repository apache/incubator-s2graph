package org.apache.s2graph.s2jobs.wal

object TestData {
  val testServiceName = "s2graph"
  val walLogsLs = Seq(
    WalLog(1L, "insert", "edge", "u1", "i1", s"$testServiceName", "click", """{"item_name":"awesome item"}"""),
    WalLog(2L, "insert", "edge", "u1", "i1", s"$testServiceName", "purchase", """{"price":2}"""),
    WalLog(3L, "insert", "edge", "u1", "q1", s"$testServiceName", "search", """{"referrer":"www.google.com"}"""),
    WalLog(4L, "insert", "edge", "u2", "i1", s"$testServiceName", "click", """{"item_name":"awesome item"}"""),
    WalLog(5L, "insert", "edge", "u2", "q2", s"$testServiceName", "search", """{"referrer":"www.bing.com"}"""),
    WalLog(6L, "insert", "edge", "u3", "i2", s"$testServiceName", "click", """{"item_name":"bad item"}"""),
    WalLog(7L, "insert", "edge", "u4", "q1", s"$testServiceName", "search", """{"referrer":"www.google.com"}""")
  )

  // order by from
  val aggExpected = Array(
    WalLogAgg("u1", Seq(
      WalLog(3L, "insert", "edge", "u1", "q1", s"$testServiceName", "search", """{"referrer":"www.google.com"}"""),
      WalLog(2L, "insert", "edge", "u1", "i1", s"$testServiceName", "purchase", """{"price":2}"""),
      WalLog(1L, "insert", "edge", "u1", "i1", s"$testServiceName", "click", """{"item_name":"awesome item"}""")
    ), 3L, 1L),
    WalLogAgg("u2", Seq(
      WalLog(5L, "insert", "edge", "u2", "q2", s"$testServiceName", "search", """{"referrer":"www.bing.com"}"""),
      WalLog(4L, "insert", "edge", "u2", "i1", s"$testServiceName", "click", """{"item_name":"awesome item"}""")
    ), 5L, 4L),
    WalLogAgg("u3", Seq(
      WalLog(6L, "insert", "edge", "u3", "i2", s"$testServiceName", "click", """{"item_name":"bad item"}""")
    ), 6L, 6L),
    WalLogAgg("u4", Seq(
      WalLog(7L, "insert", "edge", "u4", "q1", s"$testServiceName", "search", """{"referrer":"www.google.com"}""")
    ), 7L, 7L)
  )

  // order by dim, rank
  val featureDictExpected = Array(
    DimValCountRank(DimVal("click:item_name", "awesome item"), 2, 1),
    DimValCountRank(DimVal("click:item_name", "bad item"), 1, 2),
    DimValCountRank(DimVal("purchase:price", "2"), 1, 1),
    DimValCountRank(DimVal("search:referrer", "www.google.com"), 2, 1),
    DimValCountRank(DimVal("search:referrer", "www.bing.com"), 1, 2)
  )
}
