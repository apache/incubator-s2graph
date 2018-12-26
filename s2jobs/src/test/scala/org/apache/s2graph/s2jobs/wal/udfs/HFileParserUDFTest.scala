package org.apache.s2graph.s2jobs.wal.udfs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.Result
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.s2jobs.wal.utils.{DeserializeUtil, SchemaUtil}
import org.apache.spark.sql.SaveMode
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class HFileParserUDFTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {

  test("deserialize result udf.") {
    import spark.implicits._

//    val sKeyValueParser = new HFileParserUDF
//    sKeyValueParser.register(spark, "to_edge", Map(
//      "elementType" -> "indexedge",
//      "labelNames" -> "talk_friend",
//      "db.default.url" -> "jdbc:mysql://comm-s2graph.mydb.iwilab.com/graph",
//      "db.default.driver" -> "com.mysql.jdbc.Driver",
//      "db.default.user" -> "graph",
//      "db.default.password" -> "graph"
//    ))
    val config = ConfigFactory.load()
    SchemaUtil.init(config)

    val labelSchema = SchemaUtil.buildLabelSchema(Seq("talk_friend"))
    val labelSchemaBCast = sc.broadcast(labelSchema)

//    val df = spark.read.parquet("test.parquet").limit(100)
    val df = spark.read.parquet("test.parquet").as[SKeyValue].mapPartitions { iter =>
      val labelSchema = labelSchemaBCast.value

      iter.flatMap { skv =>
        val result = Result.create(Array(DeserializeUtil.sKeyValueToCell(skv)))
        DeserializeUtil.indexEdgeResultToWalsV3(result, labelSchema)
      }
    }

    df.printSchema()
    df.show(1000, false)
//
//    df.createOrReplaceTempView("kvs")
//
////    val sql = """select *, to_edge(struct(*)) as wal from kvs"""
//    val sql =
//      """
//        |SELECT   wal
//        |FROM (
//        |   SELECT   to_edge(struct(*)) as wal
//        |   FROM     kvs
//        |)
//        |WHERE    wal is not null
//      """.stripMargin
//
//    val startedAt = System.currentTimeMillis()
//    val result = spark.sql(sql)
//    //    println(s"[Count]: ${result.count()}")
////    result.write.mode(SaveMode.Overwrite).json("./parsed.json")
//        result.show(false)
//    val duration = System.currentTimeMillis() - startedAt
//    println("[Duration]: " + duration)
  }
}
