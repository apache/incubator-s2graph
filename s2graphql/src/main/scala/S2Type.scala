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

package org.apache.s2graph

import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.JsValue
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
import sangria.schema._

import scala.language.existentials
import scala.util.{Failure, Success, Try}

object S2Type {

  import sangria.schema._

  case class LabelServiceProp(name: String, columnName: String, dataType: String)

  case class MutationResponse[T](result: Try[T])

  case class PartialServiceParam(service: Service, vid: JsValue)

  case class PartialVertexParam(ts: Long,
                                id: Any,
                                props: Map[String, Any])

  case class PartialServiceVertexParam(columnName: String, vertexParam: PartialVertexParam)

  case class PartialEdgeParam(ts: Long,
                              from: Any,
                              to: Any,
                              direction: String,
                              props: Map[String, Any])

  implicit object PartialServiceVertexParamFromInput extends FromInput[Vector[PartialServiceVertexParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, marshaller.Node]]

      val ret = inputMap.toVector.map { case (columnName, node) =>
        val param = PartialVertexFromInput.fromResult(node)
        PartialServiceVertexParam(columnName, param)
      }

      ret
    }
  }

  implicit object PartialVertexFromInput extends FromInput[PartialVertexParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {

      val inputMap = node.asInstanceOf[Map[String, Any]]
      val id = inputMap("id")
      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }
      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      PartialVertexParam(ts, id, props)
    }
  }

  implicit object PartialEdgeFromInput extends FromInput[PartialEdgeParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, Any]]

      val from = inputMap("from")
      val to = inputMap("to")

      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }

      val dir = inputMap.get("direction") match {
        case Some(Some(v)) => v.asInstanceOf[String]
        case _ => "out"
      }

      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      PartialEdgeParam(ts, from, to, dir, props)
    }
  }

  implicit object IndexFromInput extends FromInput[Index] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]
      Index(input("name").asInstanceOf[String], input("propNames").asInstanceOf[Seq[String]])
    }
  }

  implicit object PropFromInput extends FromInput[Prop] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, String]]
      Prop(input("name"), input("defaultValue"), input("dataType"))
    }
  }

  implicit object LabelServiceFromInput extends FromInput[LabelServiceProp] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, String]]
      LabelServiceProp(input("name"), input("columnName"), input("dataType"))
    }
  }

  def s2TypeToScalarType(from: String): ScalarType[_] = from match {
    case "string" => StringType
    case "int" => IntType
    case "integer" => IntType
    case "long" => LongType
    case "float" => FloatType
    case "double" => FloatType
    case "boolean" => BooleanType
    case "bool" => BooleanType
  }
}

class S2Type(repo: GraphRepository) {

  import sangria.macros.derive._
  import S2Type._

  lazy val DirArg = Argument("direction", OptionInputType(DirectionType), "desc here", defaultValue = "out")

  lazy val NameArg = Argument("name", StringType, description = "desc here")

  lazy val ServiceNameArg = Argument("name", OptionInputType(ServiceListType), description = "desc here")

  lazy val ServiceNameRawArg = Argument("serviceName", ServiceListType, description = "desc here")

  lazy val ColumnNameArg = Argument("columnName", OptionInputType(ServiceColumnListType), description = "desc here")

  lazy val ColumnTypeArg = Argument("columnType", DataTypeType, description = "desc here")

  lazy val LabelNameArg = Argument("name", OptionInputType(LabelListType), description = "desc here")

  lazy val PropArg = Argument("props", OptionInputType(ListInputType(InputPropType)), description = "desc here")

  lazy val IndicesArg = Argument("indices", OptionInputType(ListInputType(InputIndexType)), description = "desc here")

  lazy val ServiceType = deriveObjectType[GraphRepository, Service](
    ObjectTypeName("Service"),
    ObjectTypeDescription("desc here"),
    RenameField("serviceName", "name"),
    AddFields(
      Field("serviceColumns", ListType(ServiceColumnType), resolve = c => c.value.serviceColumns.toList)
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

  lazy val LabelMetaType = deriveObjectType[GraphRepository, LabelMeta](
    ObjectTypeName("LabelMeta"),
    ExcludeFields("seq", "labelId")
  )

  lazy val ColumnMetaType = deriveObjectType[GraphRepository, ColumnMeta](
    ObjectTypeName("ColumnMeta"),
    ExcludeFields("seq", "columnId")
  )

  lazy val DataTypeType = EnumType(
    "DataType",
    description = Option("desc here"),
    values = List(
      EnumValue("string", value = "string"),
      EnumValue("int", value = "int"),
      EnumValue("long", value = "long"),
      EnumValue("float", value = "float"),
      EnumValue("boolean", value = "boolean")
    )
  )

  lazy val DirectionType = EnumType(
    "Direction",
    description = Option("desc here"),
    values = List(
      EnumValue("out", value = "out"),
      EnumValue("in", value = "in")
    )
  )

  lazy val InputIndexType = InputObjectType[Index](
    "Index",
    description = "desc here",
    fields = List(
      InputField("name", StringType),
      InputField("propNames", ListInputType(StringType))
    )
  )

  lazy val InputPropType = InputObjectType[Prop](
    "Prop",
    description = "desc here",
    fields = List(
      InputField("name", StringType),
      InputField("dataType", DataTypeType),
      InputField("defaultValue", StringType)
    )
  )

  lazy val dummyEnum = EnumValue("_", value = "_")

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

  lazy val CompressionAlgorithmType = EnumType(
    "CompressionAlgorithm",
    description = Option("desc here"),
    values = List(
      EnumValue("gz", description = Option("desc here"), value = "gz"),
      EnumValue("lz4", description = Option("desc here"), value = "lz4")
    )
  )

  lazy val ConsistencyLevelType = EnumType(
    "ConsistencyList",
    description = Option("desc here"),
    values = List(
      EnumValue("weak", description = Option("desc here"), value = "weak"),
      EnumValue("strong", description = Option("desc here"), value = "strong")
    )
  )

  lazy val InputLabelServiceType = InputObjectType[LabelServiceProp](
    "LabelServiceProp",
    description = "desc here",
    fields = List(
      InputField("name", ServiceListType),
      InputField("columnName", StringType),
      InputField("dataType", DataTypeType)
    )
  )

  lazy val LabelIndexType = deriveObjectType[GraphRepository, LabelIndex](
    ObjectTypeName("LabelIndex"),
    ObjectTypeDescription("desc here"),
    ExcludeFields("seq", "metaSeqs", "formulars", "labelId")
  )

  lazy val LabelType = deriveObjectType[GraphRepository, Label](
    ObjectTypeName("Label"),
    ObjectTypeDescription("desc here"),
    AddFields(
      Field("indexes", ListType(LabelIndexType), resolve = c => Nil),
      Field("props", ListType(LabelMetaType), resolve = c => Nil)
    ),
    RenameField("label", "name")
  )

  def makeInputPartialVertexParamType(service: Service,
                                      serviceColumn: ServiceColumn): InputObjectType[PartialVertexParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"${service.serviceName}_${serviceColumn.columnName}_props",
      description = "desc here",
      () => serviceColumn.metas.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      }
    )

    lazy val fields = List(
      InputField("_", OptionInputType(LongType))
    )

    InputObjectType[PartialVertexParam](
      s"${service.serviceName}_${serviceColumn.columnName}_mutate",
      description = "desc here",
      () =>
        if (serviceColumn.metas.filter(ColumnMeta.isValid).isEmpty) fields
        else List(InputField("props", OptionInputType(InputPropsType)))
    )
  }

  def makeInputPartialEdgeParamType(label: Label): InputObjectType[PartialEdgeParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"${label.label}_props",
      description = "desc here",
      () => label.labelMetaSet.toList.map { lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      }
    )

    lazy val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", s2TypeToScalarType(label.srcColumnType)),
      InputField("to", s2TypeToScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(DirectionType))
    )

    InputObjectType[PartialEdgeParam](
      s"${label.label}_mutate",
      description = "desc here",
      () =>
        if (label.labelMetaSet.isEmpty) labelFields
        else labelFields ++ Seq(InputField("props", OptionInputType(InputPropsType)))
    )
  }

  lazy val VertexArg = repo.allServices.map { service =>
    val columnArgs = service.serviceColumns.map { serviceColumn =>
      val inputParialVertexParamType = makeInputPartialVertexParamType(service, serviceColumn)
      val tpe = InputObjectType[PartialServiceVertexParam](
        serviceColumn.columnName,
        fields = List(
          InputField("id", s2TypeToScalarType(serviceColumn.columnType)),
          InputField("timestamp", OptionInputType(LongType)),
          InputField("props", OptionInputType(inputParialVertexParamType))
        )
      )

      InputField(serviceColumn.columnName, tpe)
    }

    val vertexParamType = InputObjectType[Vector[PartialServiceVertexParam]](
      s"${service.serviceName}_column",
      description = "desc here",
      fields = columnArgs.toList
    )

    Argument(service.serviceName, vertexParamType)
  }

  lazy val verticesArg = repo.allServices.flatMap { service =>
    service.serviceColumns.map { serviceColumn =>
      val inputParialVertexParamType = makeInputPartialVertexParamType(service, serviceColumn)
      Argument(serviceColumn.columnName, OptionInputType(ListInputType(inputParialVertexParamType)))
    }
  }

  lazy val EdgeArg = repo.allLabels.map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(inputPartialEdgeParamType))
  }

  lazy val EdgesArg = repo.allLabels.map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(ListInputType(inputPartialEdgeParamType)))
  }

  lazy val serviceOptArgs = List(
    "compressionAlgorithm" -> CompressionAlgorithmType,
    "cluster" -> StringType,
    "hTableName" -> StringType,
    "preSplitSize" -> IntType,
    "hTableTTL" -> IntType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }


  lazy val labelRequiredArg = List(
    "sourceService" -> InputLabelServiceType,
    "targetService" -> InputLabelServiceType
  ).map { case (name, _type) => Argument(name, _type) }

  lazy val labelOptsArgs = List(
    "serviceName" -> ServiceListType,
    "consistencyLevel" -> ConsistencyLevelType,
    "isDirected" -> BooleanType,
    "isAsync" -> BooleanType,
    "schemaVersion" -> StringType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }

  lazy val ServiceMutationResponseType = makeMutationResponseType[Service](
    "CreateService",
    "desc here",
    ServiceType
  )

  lazy val ServiceColumnMutationResponseType = makeMutationResponseType[ServiceColumn](
    "CreateServiceColumn",
    "desc here",
    ServiceColumnType
  )

  lazy val LabelMutationResponseType = makeMutationResponseType[Label](
    "CreateLabel",
    "desc here",
    LabelType
  )

  lazy val MutateResponseType = deriveObjectType[GraphRepository, MutateResponse](
    ObjectTypeName("MutateResponse"),
    ObjectTypeDescription("desc here"),
    AddFields(
      Field("isSuccess", BooleanType, resolve = c => c.value.isSuccess)
    )
  )

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
            case Success(_) => s"Created successful"
            case Failure(ex) => ex.getMessage
          }
        ),
        Field("created",
          OptionType(tpe),
          resolve = _.value.result.toOption
        )
      )
    )
  }

  lazy val vertexIdField: Field[GraphRepository, Any] = Field(
    "id",
    PlayJsonPolyType.PolyType,
    description = Some("desc here"),
    resolve = _.value match {
      case v: PartialServiceParam => v.vid
      case _ => throw new RuntimeException("dead code")
    }
  )

  lazy val tsField: Field[GraphRepository, Any] =
    Field("timestamp",
      LongType,
      description = Option("desc here"),
      resolve = _.value match {
        case e: S2EdgeLike => e.ts
        case _ => throw new RuntimeException("dead code")
      })

  def makeEdgePropFields(edgeFieldNameWithTypes: List[(String, String)]): List[Field[GraphRepository, Any]] = {
    def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
      Field(name, OptionType(tpe), description = Option("desc here"), resolve = _.value match {
        case e: S2EdgeLike =>
          val innerVal = name match {
            case "from" => e.srcForVertex.innerId
            case "to" => e.tgtForVertex.innerId
            case _ => e.propertyValue(name).get.innerVal
          }

          JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]

        case _ => throw new RuntimeException("dead code")
      })

    edgeFieldNameWithTypes.map { case (cName, cType) =>
      cType match {
        case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
        case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
        case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
        case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
        case "double" | "d" | "float64" | "float" | "f" | "float32" => makeField[Double](cName, cType, FloatType)
        case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
      }
    }
  }

  // ex: KakaoFavorites
  lazy val serviceVertexFields: List[Field[GraphRepository, Any]] = repo.allServices.map { service =>
    val serviceId = service.id.get
    val connectedLabels = repo.allLabels.filter { lb =>
      lb.srcServiceId == serviceId || lb.tgtServiceId == serviceId
    }.distinct

    // label connected on services, friends, post
    lazy val connectedLabelFields: List[Field[GraphRepository, Any]] = connectedLabels.map { label =>
      val labelColumns = List("from" -> label.srcColumnType, "to" -> label.tgtColumnType)
      val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

      lazy val EdgeType = ObjectType(label.label, () => fields[GraphRepository, Any](edgeFields ++ connectedLabelFields: _*))
      lazy val edgeFields: List[Field[GraphRepository, Any]] = tsField :: makeEdgePropFields(labelColumns ++ labelProps)
      lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
        label.label,
        ListType(EdgeType),
        arguments = DirArg :: Nil,
        description = Some("edges"),
        resolve = { c =>
          val dir = c.argOpt("direction").getOrElse("out")

          val vertex: S2VertexLike = c.value match {
            case v: S2VertexLike => v
            case e: S2Edge => if (dir == "out") e.tgtVertex else e.srcVertex
            case vp: PartialServiceParam =>
              if (dir == "out") c.ctx.partialServiceParamToVertex(label.tgtColumn, vp)
              else c.ctx.partialServiceParamToVertex(label.srcColumn, vp)
          }

          c.ctx.getEdges(vertex, label, dir)
        }
      )

      edgeTypeField
    }

    lazy val VertexType = ObjectType(
      s"${service.serviceName}",
      fields[GraphRepository, Any](vertexIdField +: connectedLabelFields: _*)
    )

    Field(
      service.serviceName,
      ListType(VertexType),
      arguments = List(
        Argument("id", OptionInputType(PlayJsonPolyType.PolyType)),
        Argument("ids", OptionInputType(ListInputType(PlayJsonPolyType.PolyType)))
      ),
      description = Some(s"serviceName: ${service.serviceName}"),
      resolve = { c =>
        val id = c.argOpt[JsValue]("id").toSeq
        val ids = c.argOpt[List[JsValue]]("ids").toList.flatten
        val svc = c.ctx.findServiceByName(service.serviceName).get

        (id ++ ids).map { vid => PartialServiceParam(svc, vid) }
      }
    ): Field[GraphRepository, Any]
  }

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

  /**
    * Query fields
    * Provide s2graph query API
    *
    * - Fields is created(or changed) for metadata is changed.
    */
  lazy val queryFields = Seq(serviceField, labelField) ++ serviceVertexFields

  /**
    * Mutation fields
    * Provide s2graph management API
    *
    * - createService
    * - createLabel
    * - addEdge
    * - addEdges
    */
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
    Field("createLabel",
      LabelMutationResponseType,
      arguments = NameArg :: PropArg :: IndicesArg :: labelRequiredArg ::: labelOptsArgs,
      resolve = c => MutationResponse(c.ctx.createLabel(c.args))
    ),
    Field("addVertex",
      OptionType(MutateResponseType),
      arguments = VertexArg,
      resolve = c => c.ctx.addVertex(c.args)
    ),
    Field("addVertices",
      ListType(MutateResponseType),
      arguments = verticesArg,
      resolve = c => c.ctx.addVertices(c.args)
    ),
    Field("addEdge",
      OptionType(MutateResponseType),
      arguments = EdgeArg,
      resolve = c => c.ctx.addEdge(c.args)
    ),
    Field("addEdges",
      ListType(MutateResponseType),
      arguments = EdgesArg,
      resolve = c => c.ctx.addEdges(c.args)
    )
  )
}
