package org.apache.s2graph.s2jobs.wal.utils

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.client.Result
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.s2jobs.wal.{SchemaManager, WalLog, WalVertex}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import play.api.libs.json.{JsObject, JsValue, Json}

class DeserializeUtilTest extends FunSuite with Matchers with BeforeAndAfterAll {

  def initTestSchemaManager(serviceName: String,
                            columnName: String,
                            labelName: String) = {
    val service = Service(Option(1), serviceName, "token", "cluster", "test", 0, None)
    val serviceLs = Seq(service)
    val serviceColumn = ServiceColumn(Option(1), service.id.get, columnName, "string", "v3", None)
    val serviceColumnLs = Seq(serviceColumn)
    val columnMeta = ColumnMeta(Option(1), serviceColumn.id.get, "age", 1, "integer", "-1")
    val columnMetaLs = Seq(columnMeta)
    val label = Label(Option(1), labelName,
      service.id.get, serviceColumn.columnName, serviceColumn.columnType,
      service.id.get, serviceColumn.columnName, serviceColumn.columnType,
      true, service.serviceName, service.id.get, "strong",
      "test", None, "v3", false, "gz", None
    )
    val labelLs = Seq(label)
    val labelMeta = LabelMeta(Option(1), label.id.get, "score", 1, "0.0", "double")
    val labelMetaLs = Seq(labelMeta)
    val labelIndex = LabelIndex(Option(1), label.id.get, "pk", 1, Seq(labelMeta.seq), "", None, None)
    val labelIndexLs = Seq(labelIndex)

    SchemaManager(serviceLs,
      serviceColumnLs,
      columnMetaLs,
      labelLs,
      labelIndexLs,
      labelMetaLs
    )
  }

  def checkEqual(walVertex: WalVertex, deserializedWalVertex: WalVertex): Boolean = {
    val msg = Seq("=" * 100, walVertex, deserializedWalVertex, "=" * 100).mkString("\n")
    println(msg)

    val expectedProps = Json.parse(walVertex.props).as[JsObject]
    val realProps = Json.parse(deserializedWalVertex.props).as[JsObject]
    val isPropsSame = expectedProps.fieldSet.forall { case (k, expectedVal) =>
      (realProps \ k).asOpt[JsValue].map { realVal => expectedVal == realVal }.getOrElse(false)
    }

    val isWalLogSame = walVertex.copy(props = "{}") == deserializedWalVertex.copy(props = "{}")

    isWalLogSame && isPropsSame
  }

  def checkEqual(walLog: WalLog, deserializedWalLog: WalLog): Boolean = {
    val msg = Seq("=" * 100, walLog, deserializedWalLog, "=" * 100).mkString("\n")
    println(msg)

    val expectedProps = Json.parse(walLog.props).as[JsObject]
    val realProps = Json.parse(deserializedWalLog.props).as[JsObject]
    val isPropsSame = expectedProps.fieldSet.forall { case (k, expectedVal) =>
      (realProps \ k).asOpt[JsValue].map { realVal => expectedVal == realVal }.getOrElse(false)
    }

    val isWalLogSame = walLog.copy(props = "{}") == deserializedWalLog.copy(props = "{}")

    isWalLogSame && isPropsSame
  }

  def createResult(kvs: Iterable[SKeyValue]): Result = {
    val cells = kvs.map { kv =>
      CellUtil.createCell(kv.row, kv.cf, kv.qualifier, kv.timestamp, Type.Put.getCode, kv.value)
    }

    Result.create(cells.toArray)
  }

  val schema = initTestSchemaManager("s1", "user", "l1")
  val tallSchemaVersions = Set("v4")
  val walLog =
    WalLog(10L, "insert", "edge", "a", "x", "s1", "l1","""{"score": 20}""")

  val walVertex =
    WalVertex(10L, "insert", "vertex", "v1", "s1", "user", """{"age": 20}""")

  test("test index edge serialize/deserialize") {
    val kvs = SerializeUtil.walToIndexEdgeKeyValue(walLog, schema, tallSchemaVersions)
    val result = createResult(kvs)
    val wals = DeserializeUtil.indexEdgeResultToWals(result, schema, tallSchemaVersions)

    wals.size shouldBe 1
    checkEqual(walLog, wals.head) shouldBe true
  }

  test("test snapshot edge serialize/deserialize") {
    val kvs = SerializeUtil.walToSnapshotEdgeKeyValue(walLog, schema, tallSchemaVersions)
    val result = createResult(kvs)
    val wals = DeserializeUtil.snapshotEdgeResultToWals(result, schema, tallSchemaVersions)

    wals.size shouldBe 1
    checkEqual(walLog, wals.head) shouldBe true
  }

  test("test vertex serialize/deserialize") {
    val kvs = SerializeUtil.walVertexToSKeyValue(walVertex, schema, tallSchemaVersions)
    val result = createResult(kvs)
    val wals = DeserializeUtil.vertexResultToWals(result, schema)

    wals.size shouldBe 1
    checkEqual(walVertex, wals.head) shouldBe true

  }
}
