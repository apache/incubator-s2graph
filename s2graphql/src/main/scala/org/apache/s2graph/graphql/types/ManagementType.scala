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

import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._

import scala.language.existentials
import scala.util.{Failure, Success, Try}
import org.apache.s2graph.graphql.types.S2Type.{ServiceColumnParam}

object ManagementType {

  import sangria.schema._

  case class MutationResponse[T](result: Try[T])

  def makeMutationResponseType[T](name: String, desc: String, tpe: ObjectType[_, T]): ObjectType[Unit, MutationResponse[T]] = {
    val retType = ObjectType(
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

    retType
  }
}

class ManagementType(repo: GraphRepository) {

  import ManagementType._
  import sangria.macros.derive._
  import org.apache.s2graph.graphql.bind.Unmarshaller._
  import org.apache.s2graph.graphql.types.StaticTypes._

  lazy val serviceColumnOnServiceWithPropInputObjectFields = repo.services().map { service =>
    InputField(service.serviceName, OptionInputType(InputObjectType(
      s"Input_${service.serviceName}_ServiceColumn_Props",
      description = "desc here",
      fields = List(
        InputField("columnName", makeServiceColumnEnumTypeOnService(service)),
        InputField("props", ListInputType(InputPropType))
      )
    )))
  }

  lazy val serviceColumnOnServiceInputObjectFields = repo.services().map { service =>
    InputField(service.serviceName, OptionInputType(InputObjectType(
      s"Input_${service.serviceName}_ServiceColumn",
      description = "desc here",
      fields = List(
        InputField("columnName", makeServiceColumnEnumTypeOnService(service))
      )
    )))
  }

  def makeServiceColumnEnumTypeOnService(service: Service): EnumType[String] = {
    val columns = service.serviceColumns(false).toList
    EnumType(
      s"Enum_${service.serviceName}_ServiceColumn",
      description = Option("desc here"),
      values = dummyEnum +: columns.map { column =>
        EnumValue(column.columnName, value = column.columnName)
      }
    )
  }

  lazy val labelPropsInputFields = repo.labels().map { label =>
    InputField(label.label, OptionInputType(InputObjectType(
      s"Input_${label.label}_props",
      description = "desc here",
      fields = List(
        InputField("props", ListInputType(InputPropType))
      )
    )))
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
        resolve = c => c.value.metasWithoutCache.filter(ColumnMeta.isValid)
      )
    )
  )

  val dummyEnum = EnumValue("_", value = "_")

  lazy val ServiceListType = EnumType(
    s"Enum_Service",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.services().map { service =>
        EnumValue(service.serviceName, value = service.serviceName)
      }
  )

  lazy val ServiceColumnListType = EnumType(
    s"Enum_ServiceColumn",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.serviceColumns().map { serviceColumn =>
        EnumValue(serviceColumn.columnName, value = serviceColumn.columnName)
      }
  )

  lazy val EnumLabelsType = EnumType(
    s"Enum_Label",
    description = Option("desc here"),
    values =
      dummyEnum +: repo.labels().map { label =>
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
    "MutateLabel",
    "desc here",
    LabelType
  )

  lazy val labelsField: Field[GraphRepository, Any] = Field(
    "Labels",
    ListType(LabelType),
    description = Option("desc here"),
    arguments = List(LabelNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.labels().filter(_.label == name)
        case None => c.ctx.labels()
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

  val AddPropServiceType = InputObjectType[ServiceColumnParam](
    "Input_Service_ServiceColumn_Props",
    description = "desc",
    fields = DummyInputField +: serviceColumnOnServiceWithPropInputObjectFields
  )

  val ServiceColumnSelectType = InputObjectType[ServiceColumnParam](
    "Input_Service_ServiceColumn",
    description = "desc",
    fields = DummyInputField +: serviceColumnOnServiceInputObjectFields
  )

  val InputServiceType = InputObjectType[ServiceColumnParam](
    "Input_Service",
    description = "desc",
    fields = DummyInputField +: serviceColumnOnServiceInputObjectFields
  )

  lazy val servicesField: Field[GraphRepository, Any] = Field(
    "Services",
    ListType(ServiceType),
    description = Option("desc here"),
    arguments = List(ServiceNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.services().filter(_.serviceName == name)
        case None => c.ctx.services()
      }
    }
  )

  /**
    * Query Fields
    * Provide s2graph management query API
    */
  lazy val queryFields: List[Field[GraphRepository, Any]] = List(servicesField, labelsField)

  /**
    * Mutation fields
    * Provide s2graph management mutate API
    *
    * - createService
    * - createLabel
    * - ...
    */

  lazy val labelRequiredArg = List(
    Argument("sourceService", InputServiceType),
    Argument("targetService", InputServiceType)
  )

  val labelOptsArgs = List(
    Argument("serviceName", OptionInputType(ServiceListType)),
    Argument("consistencyLevel", OptionInputType(ConsistencyLevelType)),
    Argument("isDirected", OptionInputType(BooleanType)),
    Argument("isAsync", OptionInputType(BooleanType)),
    Argument("schemaVersion", OptionInputType(StringType))
  )

  val NameArg = Argument("name", StringType, description = "desc here")

  lazy val ServiceNameArg = Argument("name", OptionInputType(ServiceListType), description = "desc here")

  lazy val ServiceNameRawArg = Argument("serviceName", ServiceListType, description = "desc here")

  lazy val ColumnNameArg = Argument("columnName", OptionInputType(ServiceColumnListType), description = "desc here")

  lazy val ColumnTypeArg = Argument("columnType", DataTypeType, description = "desc here")

  lazy val LabelNameArg = Argument("name", OptionInputType(EnumLabelsType), description = "desc here")

  lazy val PropArg = Argument("props", OptionInputType(ListInputType(InputPropType)), description = "desc here")

  lazy val IndicesArg = Argument("indices", OptionInputType(ListInputType(InputIndexType)), description = "desc here")

  lazy val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("createService",
      ServiceMutationResponseType,
      arguments = NameArg :: serviceOptArgs,
      resolve = c => MutationResponse(c.ctx.createService(c.args))
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
    ),
    Field("createServiceColumn",
      ServiceColumnMutationResponseType,
      arguments = List(ServiceNameRawArg, Argument("columnName", StringType), ColumnTypeArg, PropArg),
      resolve = c => MutationResponse(c.ctx.createServiceColumn(c.args))
    ),
    Field("deleteServiceColumn",
      ServiceColumnMutationResponseType,
      arguments = Argument("service", ServiceColumnSelectType) :: Nil,
      resolve = c => MutationResponse(c.ctx.deleteServiceColumn(c.args))
    ),
    Field("addPropsToServiceColumn",
      ServiceColumnMutationResponseType,
      arguments = Argument("service", AddPropServiceType) :: Nil,
      resolve = c => MutationResponse(c.ctx.addPropsToServiceColumn(c.args))
    ),
    Field("addPropsToLabel",
      LabelMutationResponseType,
      arguments = Argument("labelName", EnumLabelsType) :: PropArg :: Nil,
      resolve = c => MutationResponse(c.ctx.addPropsToLabel(c.args))
    )
  )
}
