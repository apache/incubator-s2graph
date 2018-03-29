package org.apache.s2graph.s2jobs.task

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class ProcessTest extends FunSuite with DataFrameSuiteBase {

  test("SqlProcess execute sql") {
    import spark.implicits._

    val inputDF = Seq(
      ("a", "b", "friend"),
      ("a", "c", "friend"),
      ("a", "d", "friend")
    ).toDF("from", "to", "label")

    val inputMap = Map("input" -> inputDF)
    val sql = "SELECT * FROM input WHERE to = 'b'"
    val conf = TaskConf("test", "sql", Seq("input"), Map("sql" -> sql))

    val process = new SqlProcess(conf)

    val rstDF = process.execute(spark, inputMap)
    val tos = rstDF.collect().map{ row => row.getAs[String]("to")}

    assert(tos.size == 1)
    assert(tos.head == "b")
  }
}
