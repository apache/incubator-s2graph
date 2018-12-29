/*
package org.apache.s2graph.s2jobs.wal.udfs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.Result
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.s2jobs.task._
import org.apache.s2graph.s2jobs.wal.utils.{DeserializeUtil, SchemaUtil}
import org.apache.spark.sql.{Encoders, Row, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.reflect.runtime.universe._

class HFileParserUDFTest extends FunSuite with Matchers with BeforeAndAfterAll with DataFrameSuiteBase {

  test("transform") {
    import spark.implicits._
//    implicit val tt = typeTag[HResult]
//    implicit val enc = Encoders.kryo[HResult]

    val outputPath = "transform.parquet"
    val df =
      spark.read.parquet("direct.parquet").limit(100)

    val results = df.map { row =>
      val kvs = row.getAs[Seq[Row]]("kvs").map { row =>
        DeserializeUtil.sKeyValueFromRow(row)
      }

      val table = kvs.head.table
      val rowKey = kvs.head.row
      val cf = kvs.head.cf

      val partialKVs = kvs.map { kv => PartialKeyValue(kv.qualifier, kv.value, kv.timestamp) }
      HResult(table, rowKey, cf, partialKVs)
    }

    results.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  test("deserialize result udf.") {
    import spark.implicits._

    val sKeyValueParser = new HFileParserUDF
    sKeyValueParser.register(spark, "to_wal", Map(
      "elementType" -> "indexedge",
      "labelNames" -> "talk_friend",
      "db.default.url" -> "jdbc:mysql://comm-s2graph.mydb.iwilab.com/graph",
      "db.default.driver" -> "com.mysql.jdbc.Driver",
      "db.default.user" -> "graph",
      "db.default.password" -> "graph"
    ))

    val df =
      spark.read.parquet("transform.parquet")

    df.printSchema()
    df.createOrReplaceTempView("results")


    val sql =
      """
        |SELECT wal.*
        |FROM   (
        |   SELECT  explode(to_wal(row, kvs)) as wal
        |   FROM    results
        |)
      """.stripMargin


    val result = spark.sql(sql)
    result.printSchema()

    result.write.mode(SaveMode.Overwrite).json("./parsed.json")
  }
}
*/
