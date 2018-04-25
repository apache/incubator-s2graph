/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core.io

import org.apache.s2graph.core.{EdgeId, JSONParser, S2VertexPropertyId}
import org.apache.s2graph.core.schema.{ColumnMeta, Service, ServiceColumn}
import org.apache.s2graph.core.types.{HBaseType, InnerValLike, VertexId}
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.functional.syntax._

object Conversions {
  /* Serializer for inner value class */
//  implicit object InnerValLikeReads extends Reads[InnerValLike] {
//    def reads(json: JsValue) = {
//      val value = (json \ "value").as[JsValue]
//      val dataType = (json \ "dataType").as[String]
//      val schemaVersion = (json \ "schemaVersion").as[String]
//      val innerVal = JSONParser.jsValueToInnerVal(value, dataType, schemaVersion).get
//      JsSuccess(innerVal)
//    }
//  }
//  implicit object InnerValLikeWrites extends Writes[InnerValLike] {
//    override def writes(o: InnerValLike): JsValue = {
//      Json.obj("value" -> JSONParser.anyValToJsValue(o.value),
//        "dataType" -> o.dataType,
//        "schemaVersion" -> o.schemaVersion)
//    }
//  }

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
      (JsPath \ "dataType").read[String] and
      (JsPath \ "defaultValue").read[String] and
      (JsPath \ "storeGlobalIndex").read[Boolean]
    )(ColumnMeta.apply _)

  implicit val columnMetaWrites: Writes[ColumnMeta] = (
    (JsPath \ "id").writeNullable[Int] and
      (JsPath \ "columnId").write[Int] and
      (JsPath \ "name").write[String] and
      (JsPath \ "seq").write[Byte] and
      (JsPath \ "dataType").write[String] and
      (JsPath \ "defaultValue").write[String] and
      (JsPath \ "storeGlobalIndex").write[Boolean]
    )(unlift(ColumnMeta.unapply))

  /* Graph Class */
  implicit object S2VertexPropertyIdReads extends Reads[S2VertexPropertyId] {
    override def reads(json: JsValue): JsResult[S2VertexPropertyId] = {
      val columnMeta = columnMetaReads.reads((json \ "columnMeta").get).get
      val innerVal = JSONParser.jsValueToInnerVal((json \ "value").get,
        columnMeta.dataType, HBaseType.DEFAULT_VERSION).get
      JsSuccess(S2VertexPropertyId(columnMeta, innerVal))
    }
  }
  implicit object S2VertexPropertyIdWrites extends Writes[S2VertexPropertyId] {
    override def writes(o: S2VertexPropertyId): JsValue = {
      Json.obj("columnMeta" -> columnMetaWrites.writes(o.columnMeta),
        "value" -> JSONParser.anyValToJsValue(o.value.value).get)
    }
  }
  implicit val s2VertexPropertyIdReads: Reads[S2VertexPropertyId] = S2VertexPropertyIdReads
//    (
//    (JsPath \ "column").read[ColumnMeta] and
//      (JsPath \ "value").read[InnerValLike]
//    )(S2VertexPropertyId.apply _)

  implicit val s2VertexPropertyIdWrites: Writes[S2VertexPropertyId] = S2VertexPropertyIdWrites
//    (
//    (JsPath \ "column").write[ColumnMeta] and
//      (JsPath \ "value").write[InnerValLike]
//    )(unlift(S2VertexPropertyId.unapply))

  implicit object S2VertexIdReads extends Reads[VertexId] {
    override def reads(json: JsValue): JsResult[VertexId] = {
      val column = serviceColumnReads.reads((json \ "column").get).get
      val valueJson = (json \ "value").get
      val innerVal = JSONParser.jsValueToInnerVal(valueJson, column.columnType, column.schemaVersion).get

      JsSuccess(VertexId(column, innerVal))
    }
  }
  implicit object S2VertexIdWrites extends Writes[VertexId] {
    override def writes(o: VertexId): JsValue = {
      Json.obj(
        "column" -> serviceColumnWrites.writes(o.column),
        "value" -> JSONParser.anyValToJsValue(o.innerId.value)
      )
    }
  }
  implicit val s2VertexIdReads: Reads[VertexId] = S2VertexIdReads
//    (
//    (JsPath \ "column").read[ServiceColumn] and
//      (JsPath \ "value").read[InnerValLike]
//    )(VertexId.apply _)

  implicit val s2VertexIdWrites: Writes[VertexId] = S2VertexIdWrites
//    (
//    (JsPath \ "column").write[ServiceColumn] and
//      (JsPath \ "value").write[InnerValLike]
//    )(unlift(VertexId.unapply))

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
