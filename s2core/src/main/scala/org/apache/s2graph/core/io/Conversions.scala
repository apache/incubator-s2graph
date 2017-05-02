package org.apache.s2graph.core.io

import org.apache.s2graph.core.{EdgeId, JSONParser, S2VertexPropertyId}
import org.apache.s2graph.core.mysqls.{ColumnMeta, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerValLike, VertexId}
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.functional.syntax._

object Conversions {
  /* Serializer for inner value class */
  implicit object InnerValLikeReads extends Reads[InnerValLike] {
    def reads(json: JsValue) = {
      val value = (json \ "value").as[JsValue]
      val dataType = (json \ "dataType").as[String]
      val schemaVersion = (json \ "schemaVersion").as[String]
      val innerVal = JSONParser.jsValueToInnerVal(value, dataType, schemaVersion).get
      JsSuccess(innerVal)
    }
  }
  implicit object InnerValLikeWrites extends Writes[InnerValLike] {
    override def writes(o: InnerValLike): JsValue = {
      Json.obj("value" -> JSONParser.anyValToJsValue(o.value),
        "dataType" -> o.dataType,
        "schemaVersion" -> o.schemaVersion)
    }
  }

  /* Serializer for Models */

  implicit val serviceReads: Reads[Service] = (
    (JsPath \ "id").readNullable[Int] and
      (JsPath \ "serviceName").read[String] and
      (JsPath \ "accessToken").read[String] and
      (JsPath \ "cluster").read[String] and
      (JsPath \ "hTableName").read[String] and
      (JsPath \ "preSplitSize").read[Int] and
      (JsPath \ "hTableTTL").readNullable[Int] and
      (JsPath \ "options").readNullable[String]
    )(Service.apply _)

  implicit val serviceWrites: Writes[Service] = (
    (JsPath \ "id").writeNullable[Int] and
      (JsPath \ "serviceName").write[String] and
      (JsPath \ "accessToken").write[String] and
      (JsPath \ "cluster").write[String] and
      (JsPath \ "hTableName").write[String] and
      (JsPath \ "preSplitSize").write[Int] and
      (JsPath \ "hTableTTL").writeNullable[Int] and
      (JsPath \ "options").writeNullable[String]
    )(unlift(Service.unapply))

  implicit val serviceColumnReads: Reads[ServiceColumn] = (
    (JsPath \ "id").readNullable[Int] and
      (JsPath \ "serviceId").read[Int] and
      (JsPath \ "columnName").read[String] and
      (JsPath \ "columnType").read[String] and
      (JsPath \ "schemaVersion").read[String]
    )(ServiceColumn.apply _)

  implicit val serviceColumnWrites: Writes[ServiceColumn] = (
    (JsPath \ "id").writeNullable[Int] and
      (JsPath \ "serviceId").write[Int] and
      (JsPath \ "columnName").write[String] and
      (JsPath \ "columnType").write[String] and
      (JsPath \ "schemaVersion").write[String]
    )(unlift(ServiceColumn.unapply))

  implicit val columnMetaReads: Reads[ColumnMeta] = (
    (JsPath \ "id").readNullable[Int] and
      (JsPath \ "columnId").read[Int] and
      (JsPath \ "name").read[String] and
      (JsPath \ "seq").read[Byte] and
      (JsPath \ "dataType").read[String]
    )(ColumnMeta.apply _)

  implicit val columnMetaWrites: Writes[ColumnMeta] = (
    (JsPath \ "id").writeNullable[Int] and
      (JsPath \ "columnId").write[Int] and
      (JsPath \ "name").write[String] and
      (JsPath \ "seq").write[Byte] and
      (JsPath \ "dataType").write[String]
    )(unlift(ColumnMeta.unapply))

  /* Graph Class */
  implicit val s2VertexPropertyIdReads: Reads[S2VertexPropertyId] = (
    (JsPath \ "column").read[ColumnMeta] and
      (JsPath \ "value").read[InnerValLike]
    )(S2VertexPropertyId.apply _)

  implicit val s2VertexPropertyIdWrites: Writes[S2VertexPropertyId] = (
    (JsPath \ "column").write[ColumnMeta] and
      (JsPath \ "value").write[InnerValLike]
    )(unlift(S2VertexPropertyId.unapply))

  implicit val s2VertexIdReads: Reads[VertexId] = (
    (JsPath \ "column").read[ServiceColumn] and
      (JsPath \ "value").read[InnerValLike]
    )(VertexId.apply _)

  implicit val s2VertexIdWrites: Writes[VertexId] = (
    (JsPath \ "column").write[ServiceColumn] and
      (JsPath \ "value").write[InnerValLike]
    )(unlift(VertexId.unapply))

  implicit val s2EdgeIdReads: Reads[EdgeId] = (
    (JsPath \ "srcVertexId").read[VertexId] and
      (JsPath \ "tgtVertexId").read[VertexId] and
      (JsPath \ "labelName").read[String] and
      (JsPath \ "direction").read[String] and
      (JsPath \ "ts").read[Long]
    )(EdgeId.apply _)

  implicit val s2EdgeIdWrites: Writes[EdgeId] = (
    (JsPath \ "srcVertexId").write[VertexId] and
      (JsPath \ "tgtVertexId").write[VertexId] and
      (JsPath \ "labelName").write[String] and
      (JsPath \ "direction").write[String] and
      (JsPath \ "ts").write[Long]
    )(unlift(EdgeId.unapply))

}
