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

package org.apache.s2graph.graphql.types

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.graphql._
import org.apache.s2graph.graphql.repository.GraphRepository
import play.api.libs.json.JsValue
import sangria.marshalling._
import sangria.schema._

import scala.language.existentials
import scala.util.{Failure, Success, Try}
import org.apache.s2graph.graphql.marshaller._
import org.apache.s2graph.graphql.types.S2Type.PartialServiceColumn

object S2ManagementType {

  import sangria.schema._

  case class PropWithColumn(name: String, Props: Vector[Prop])

  case class MutationResponse[T](result: Try[T])

  def makeMutationResponseType[T](name: String, desc: String, tpe: ObjectType[_, T]) = {
    ObjectType(
      name,
      desc,
      () => fields[Unit, MutationResponse[T]](
        Field("isSuccess",
          BooleanType,
          resolve = _.value.result.isSuccess
        ),
        Field("message",
          StringType,
          resolve = _.value.result match {
            case Success(_) => s"Mutation successful"
            case Failure(ex) => ex.getMessage
          }
        ),
        Field("object",
          OptionType(tpe),
          resolve = _.value.result.toOption
        )
      )
    )
  }
}

class S2ManagementType(repo: GraphRepository) {

  import S2ManagementType._

  import sangria.macros.derive._

  lazy val serviceColumnOnServiceWithPropInputObjectFields = repo.allServices.map { service =>
    InputField(service.serviceName, OptionInputType(InputObjectType(
      s"columnWithProp",
      description = "desc here",
      fields = List(
        InputField("columnName", makeServiceColumnEnumTypeOnService(service)),
        InputField("props", ListInputType(InputPropType))
      )
    )))
  }

  lazy val serviceColumnOnServiceInputObjectFields = repo.allServices.map { service =>
    InputField(service.serviceName, OptionInputType(InputObjectType(
      s"column",
      description = "desc here",
      fields = List(
        InputField("columnName", makeServiceColumnEnumTypeOnService(service))
      )
    )))
  }

  def makeServiceColumnEnumTypeOnService(service: Service): EnumType[String] = {
    val columns = service.serviceColumns(false).toList
    EnumType(
      s"${service.serviceName}_columns",
      description = Option("desc here"),
      values = dummyEnum +: columns.map { column =>
        EnumValue(column.columnName, value = column.columnName)
      }
    )
  }

  lazy val ServiceType = deriveObjectType[GraphRepository, Service](
    ObjectTypeName("Service"),
    ObjectTypeDescription("desc here"),
    RenameField("serviceName", "name"),
    AddFields(
      Field("serviceColumns", ListType(ServiceColumnType), resolve = c => c.value.serviceColumns(false).toList)
    )
  )

  lazy val ServiceColumnType = deriveObjectType[GraphRepository, ServiceColumn](
    ObjectTypeName("ServiceColumn"),
    ObjectTypeDescription("desc here"),
    RenameField("columnName", "name"),
    AddFields(
      Field("props", ListType(ColumnMetaType),
        resolve = c => c.value.metas.filter(ColumnMeta.isValid))
    )
  )

  val dummyEnum = EnumValue("_", value = "_")

  lazy val ServiceListType = EnumType(
    s"ServiceList",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.allServices.map { service =>
        EnumValue(service.serviceName, value = service.serviceName)
      }
  )

  lazy val ServiceColumnListType = EnumType(
    s"ServiceColumnList",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.allServiceColumns.map { serviceColumn =>
        EnumValue(serviceColumn.columnName, value = serviceColumn.columnName)
      }
  )

  lazy val LabelListType = EnumType(
    s"LabelList",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.allLabels.map { label =>
        EnumValue(label.label, value = label.label)
      }
  )

  lazy val ServiceMutationResponseType = makeMutationResponseType[Service](
    "MutateService",
    "desc here",
    ServiceType
  )

  lazy val ServiceColumnMutationResponseType = makeMutationResponseType[ServiceColumn](
    "MutateServiceColumn",
    "desc here",
    ServiceColumnType
  )

  lazy val LabelMutationResponseType = makeMutationResponseType[Label](
    "MutateLabelLabel",
    "desc here",
    LabelType
  )

  lazy val serviceColumnField: Field[GraphRepository, Any] = Field(
    "ServiceColumn",
    ListType(ServiceColumnType),
    description = Option("desc here"),
    arguments = List(ServiceNameRawArg, ColumnNameArg, PropArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.allServiceColumns.filter(_.columnName == name)
        case None => c.ctx.allServiceColumns
      }
    }
  )

  lazy val labelField: Field[GraphRepository, Any] = Field(
    "Labels",
    ListType(LabelType),
    description = Option("desc here"),
    arguments = List(LabelNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.allLabels.filter(_.label == name)
        case None => c.ctx.allLabels
      }
    }
  )

  val serviceOptArgs = List(
    "compressionAlgorithm" -> CompressionAlgorithmType,
    "cluster" -> StringType,
    "hTableName" -> StringType,
    "preSplitSize" -> IntType,
    "hTableTTL" -> IntType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }

  val AddPropServiceType = InputObjectType[Vector[PartialServiceColumn]](
    "serviceName",
    description = "desc",
    fields = serviceColumnOnServiceWithPropInputObjectFields
  )

  val ServiceColumnSelectType = InputObjectType[PartialServiceColumn](
    "serviceColumnSelect",
    description = "desc",
    fields = serviceColumnOnServiceInputObjectFields
  )

  val SourceServiceType = InputObjectType[PartialServiceColumn](
    "sourceService",
    description = "desc",
    fields = serviceColumnOnServiceInputObjectFields
  )

  val TargetServiceType = InputObjectType[PartialServiceColumn](
    "sourceService",
    description = "desc",
    fields = serviceColumnOnServiceInputObjectFields
  )

  lazy val labelRequiredArg = List(
    "sourceService" -> SourceServiceType,
    "targetService" -> TargetServiceType
  ).map { case (name, _type) => Argument(name, _type) }

  val labelOptsArgs = List(
    "serviceName" -> ServiceListType,
    "consistencyLevel" -> ConsistencyLevelType,
    "isDirected" -> BooleanType,
    "isAsync" -> BooleanType,
    "schemaVersion" -> StringType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }


  /**
    * Management query
    */

  lazy val serviceField: Field[GraphRepository, Any] = Field(
    "Services",
    ListType(ServiceType),
    description = Option("desc here"),
    arguments = List(ServiceNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.allServices.filter(_.serviceName == name)
        case None => c.ctx.allServices
      }
    }
  )

  lazy val queryFields: List[Field[GraphRepository, Any]] = List(serviceField, labelField)

  /**
    * Mutation fields
    * Provide s2graph management API
    *
    * - createService
    * - createLabel
    * - ...
    */

  val NameArg = Argument("name", StringType, description = "desc here")

  lazy val ServiceNameArg = Argument("name", OptionInputType(ServiceListType), description = "desc here")

  lazy val ServiceNameRawArg = Argument("serviceName", ServiceListType, description = "desc here")

  lazy val ColumnNameArg = Argument("columnName", OptionInputType(ServiceColumnListType), description = "desc here")

  lazy val ColumnTypeArg = Argument("columnType", DataTypeType, description = "desc here")

  lazy val LabelNameArg = Argument("name", OptionInputType(LabelListType), description = "desc here")

  lazy val PropArg = Argument("props", OptionInputType(ListInputType(InputPropType)), description = "desc here")

  lazy val IndicesArg = Argument("indices", OptionInputType(ListInputType(InputIndexType)), description = "desc here")

  lazy val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("createService",
      ServiceMutationResponseType,
      arguments = NameArg :: serviceOptArgs,
      resolve = c => MutationResponse(c.ctx.createService(c.args))
    ),
    Field("createServiceColumn",
      ServiceColumnMutationResponseType,
      arguments = List(ServiceNameRawArg, Argument("columnName", StringType), ColumnTypeArg, PropArg),
      resolve = c => MutationResponse(c.ctx.createServiceColumn(c.args))
    ),
    Field("addPropsToServiceColumn",
      ListType(ServiceColumnMutationResponseType),
      arguments = Argument("serviceName", AddPropServiceType) :: Nil,
      resolve = c => c.ctx.addPropsOnServiceColumn(c.args) map (MutationResponse(_))
    ),
    Field("deleteServiceColumn",
      ListType(ServiceColumnMutationResponseType),
      arguments = Argument("serviceName", ServiceColumnSelectType) :: Nil,
      resolve = c => c.ctx.deleteServiceColumn(c.args).map(MutationResponse(_))
    ),
    Field("createLabel",
      LabelMutationResponseType,
      arguments = NameArg :: PropArg :: IndicesArg :: labelRequiredArg ::: labelOptsArgs,
      resolve = c => MutationResponse(c.ctx.createLabel(c.args))
    ),
    Field("deleteLabel",
      LabelMutationResponseType,
      arguments = LabelNameArg :: Nil,
      resolve = c => MutationResponse(c.ctx.deleteLabel(c.args))
    )
  )
}
