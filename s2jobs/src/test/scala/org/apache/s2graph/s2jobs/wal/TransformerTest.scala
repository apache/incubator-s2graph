package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.transformer._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

class TransformerTest extends FunSuite with Matchers {
  val walLog = WalLog(1L, "insert", "edge", "a", "b", "s2graph", "friends", """{"name": 1, "url": "www.google.com"}""")

  test("test default transformer") {
    val taskConf = TaskConf.Empty
    val transformer = new DefaultTransformer(taskConf)
    val dimVals = transformer.toDimValLs(walLog, "name", "1")

    dimVals shouldBe Seq(DimVal("friends:name", "1"))
  }

  test("test ExtractDomain from URL") {
    val taskConf = TaskConf.Empty.copy(options =
      Map("urlDimensions" -> Json.toJson(Seq("url")).toString())
    )
    val transformer = new ExtractDomain(taskConf)
    val dimVals = transformer.toDimValLs(walLog, "url", "http://www.google.com/abc")

    dimVals shouldBe Seq(
      DimVal("host", "www.google.com"),
      DimVal("domain", "www.google.com"),
      DimVal("domain", "www.google.com/abc")
    )
  }
}
