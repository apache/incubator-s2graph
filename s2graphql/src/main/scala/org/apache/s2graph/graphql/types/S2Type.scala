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
import org.apache.s2graph.core.schema._
import org.apache.s2graph.graphql
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._
import org.apache.s2graph.graphql.types.StaticTypes._

import scala.language.existentials

object S2Type {

  case class EdgeQueryParam(v: S2VertexLike, qp: QueryParam)

  case class AddVertexParam(timestamp: Long,
                            id: Any,
                            columnName: String,
                            props: Map[String, Any])

  case class AddEdgeParam(ts: Long,
                          from: Any,
                          to: Any,
                          direction: String,
                          props: Map[String, Any])

  // Management params
  case class ServiceColumnParam(serviceName: String,
                                columnName: String,
                                props: Seq[Prop] = Nil)

  def makeGraphElementField(cName: String, cType: String): Field[GraphRepository, Any] = {
    def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
      Field(name,
        OptionType(tpe),
        description = Option("desc here"),
        resolve = c => FieldResolver.graphElement[A](name, cType, c)
      )

    cType match {
      case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
      case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
      case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
      case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
      case "double" | "d" => makeField[Double](cName, cType, FloatType)
      case "float64" | "float" | "f" | "float32" => makeField[Double](cName, "double", FloatType)
      case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
    }
  }

  def makeInputFieldsOnService(service: Service)(implicit repo: GraphRepository): Seq[InputField[Any]] = {
    val serviceColumns = repo.serviceColumnMap(service)

    val inputFields = serviceColumns.map { serviceColumn =>
      val columnMetas = repo.columnMetaMap(serviceColumn)
      val idField = InputField("id", toScalarType(serviceColumn.columnType))
      val propFields = columnMetas.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name.toValidName, OptionInputType(toScalarType(lm.dataType)))
      }

      val vertexMutateType = InputObjectType[Map[String, Any]](
        s"Input_${service.serviceName.toValidName}_${serviceColumn.columnName.toValidName}_vertex_mutate",
        description = "desc here",
        () => idField :: propFields
      )

      InputField[Any](serviceColumn.columnName.toValidName, OptionInputType(ListInputType(vertexMutateType)))
    }

    inputFields
  }

  def makeInputFieldsOnLabel(label: Label)(implicit repo: GraphRepository): Seq[InputField[Any]] = {
    val labelMetaSet = repo.labelMetaMap(label)

    val propFields = labelMetaSet.filterNot(_.name == "timestamp").map { lm =>
      InputField(lm.name.toValidName, OptionInputType(toScalarType(lm.dataType)))
    }

    val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", toScalarType(label.srcColumnType)),
      InputField("to", toScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(BothDirectionType))
    )

    labelFields.asInstanceOf[Seq[InputField[Any]]] ++ propFields.asInstanceOf[Seq[InputField[Any]]]
  }

  def makeServiceColumnFields(column: ServiceColumn)
                             (implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {

    val columnMetas = repo.columnMetaMap(column)
    val relatedLabels = repo.columnLabelMap(column)

    val reservedFields = Vector("id" -> column.columnType, "timestamp" -> "long")
    val columnMetasKv = columnMetas.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }

    val (sameLabel, diffLabel) = relatedLabels.toList.partition(l => l.srcColumn == l.tgtColumn)

    val outLabels = diffLabel.filter(l => column == l.srcColumn).distinct
    val inLabels = diffLabel.filter(l => column == l.tgtColumn).distinct
    val inOutLabels = sameLabel.filter(l => l.srcColumn == column && l.tgtColumn == column)

    val columnFields = reservedFields.map { case (k, v) => makeGraphElementField(k.toValidName, v) }
    val propFields = columnMetasKv.map { case (k, v) => makeGraphElementField(k.toValidName, v) }

    val outLabelFields: List[Field[GraphRepository, Any]] = outLabels.map(l => toLabelFieldOnColumn("out", l))
    val inLabelFields: List[Field[GraphRepository, Any]] = inLabels.map(l => toLabelFieldOnColumn("in", l))
    val inOutLabelFields: List[Field[GraphRepository, Any]] = inOutLabels.map(l => toLabelFieldOnColumn("both", l))

    val propsType =
      if (propFields.isEmpty) Nil
      else List(wrapField(
        s"ServiceColumn_${column.service.serviceName.toValidName}_${column.columnName.toValidName}_props", "props", propFields))

    lazy val labelFieldNameSet = (outLabels ++ inLabels ++ inOutLabels).map(_.label.toValidName).toSet

    propsType ++ inLabelFields ++ outLabelFields ++ inOutLabelFields ++ columnFields
  }

  def toLabelFieldOnColumn(dir: String, label: Label)
                          (implicit repo: GraphRepository): Field[GraphRepository, Any] = {

    val LabelType = makeLabelType(dir, label)

    val dirArgs = dir match {
      case "in" => Argument("direction", OptionInputType(InDirectionType), "desc here", defaultValue = "in") :: Nil
      case "out" => Argument("direction", OptionInputType(OutDirectionType), "desc here", defaultValue = "out") :: Nil
      case "both" => Argument("direction", OptionInputType(BothDirectionType), "desc here", defaultValue = "out") :: Nil
    }

    val indices = repo.labelIndiceMap(label)

    val indexEnumType = EnumType(
      s"Label_Index_${label.label.toValidName}",
      description = Option("desc here"),
      values =
        if (indices.isEmpty) EnumValue("_", value = "_") :: Nil
        else indices.map(idx => EnumValue(idx.name.toValidName, value = idx.name))
    )

    val paramArgs = List(
      Argument("offset", OptionInputType(IntType), "desc here", defaultValue = 0),
      Argument("limit", OptionInputType(IntType), "desc here", defaultValue = 100),
      Argument("index", OptionInputType(indexEnumType), "desc here"),
      Argument("filter", OptionInputType(StringType), "desc here")
    )

    val edgeTypeField: Field[GraphRepository, Any] = Field(
      s"${label.label.toValidName}",
      ListType(LabelType),
      arguments = dirArgs ++ paramArgs,
      description = Some("fetch edges"),
      resolve = { c =>
        implicit val ec = c.ctx.ec

        val edgeQueryParam = graphql.types.FieldResolver.label(label, c)
        val empty = Seq.empty[S2EdgeLike]

        DeferredValue(
          GraphRepository.edgeFetcher.deferOpt(edgeQueryParam)
        ).map(m => m.fold(empty)(m => m._2))
      }
    )

    edgeTypeField
  }


  def makeColumnType(column: ServiceColumn)
                    (implicit repo: GraphRepository): ObjectType[GraphRepository, Any] = {

    val objectName = s"ServiceColumn_${column.service.serviceName.toValidName}_${column.columnName.toValidName}"

    lazy val serviceColumnFields = makeServiceColumnFields(column)

    val ColumnType = ObjectType(
      objectName,
      () => fields[GraphRepository, Any](serviceColumnFields: _*)
    )

    ColumnType
  }

  def makeServiceType(service: Service)(implicit repo: GraphRepository): ObjectType[GraphRepository, Any] = {

    val _serviceFields = makeServiceFields(service)
    val serviceFields = if (_serviceFields.isEmpty) DummyObjectTypeField :: _serviceFields else _serviceFields

    ObjectType(
      s"Service_${service.serviceName.toValidName}",
      fields[GraphRepository, Any](serviceFields: _*)
    )
  }

  def makeServiceFields(service: Service)(implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {

    val serviceColumns = repo.serviceColumnMap(service)
    val columnsOnService = serviceColumns.map { column =>

      val ColumnType = makeColumnType(column)

      Field(column.columnName.toValidName,
        ListType(ColumnType),
        arguments = List(
          Argument("id", OptionInputType(toScalarType(column.columnType))),
          Argument("ids", OptionInputType(ListInputType(toScalarType(column.columnType)))),
          Argument("search", OptionInputType(StringType)),
          Argument("offset", OptionInputType(IntType), defaultValue = 0),
          Argument("limit", OptionInputType(IntType), defaultValue = 100),
          Argument("filter", OptionInputType(StringType), "desc here")
        ),
        description = Option("desc here"),
        resolve = c => {
          implicit val ec = c.ctx.ec

          val vertexQueryParam = FieldResolver.serviceColumnOnService(column, c)
          DeferredValue(GraphRepository.vertexFetcher.defer(vertexQueryParam)).map(m => m._2)
        }
      ): Field[GraphRepository, Any]
    }

    columnsOnService.toList
  }

  def makeLabelType(dir: String, label: Label)
                   (implicit repo: GraphRepository): ObjectType[GraphRepository, Any] = {

    val objectName = s"Label_${label.label.toValidName}_${dir}"

    lazy val labelFields = makeLabelFields(dir, label)

    val LabelType = ObjectType(
      objectName,
      () => fields[GraphRepository, Any](labelFields: _*)
    )

    LabelType
  }

  def makeLabelFields(dir: String, label: Label)
                     (implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {

    val relatedMetas = repo.labelMetaMap(label)
    val labelReserved = List("direction" -> "string", "timestamp" -> "long")

    val labelProps = relatedMetas
      .filterNot(l => labelReserved.exists(kv => kv._1 == l.name))
      .map { lm => lm.name -> lm.dataType }

    val column = if (dir == "out") label.tgtColumn else label.srcColumn

    val labelFields = labelReserved.map { case (k, v) => makeGraphElementField(k.toValidName, v) }
    val propFields = labelProps.map { case (k, v) => makeGraphElementField(k.toValidName, v) }

    val labelPropField =
      if (propFields.isEmpty) Nil
      else List(wrapField(s"Label_${label.label.toValidName}_props", "props", propFields))

    val labelColumnType = makeColumnType(column)

    val serviceColumnField: Field[GraphRepository, Any] =
      Field(column.columnName.toValidName, labelColumnType, resolve = c => {
        implicit val ec = c.ctx.ec
        val vertexQueryParam = FieldResolver.serviceColumnOnLabel(c)

        DeferredValue(GraphRepository.vertexFetcher.defer(vertexQueryParam)).map(m => m._2.head)
      })

    List(serviceColumnField) ++ labelPropField ++ labelFields.filterNot(_.name.toValidName == column.columnName.toValidName)
  }

  def services(): List[Service] = {
    Service.findAll().distinct
  }

  def serviceColumns(): List[ServiceColumn] = {
    val allServices = services().toSet

    ServiceColumn
      .findAll()
      .filter(sc => allServices(sc.service))
      .distinct
  }

  def labels() = {
    val allServiceColumns = serviceColumns().toSet

    Label
      .findAll()
      .filter(l => allServiceColumns(l.srcColumn) || allServiceColumns(l.tgtColumn))
      .distinct
  }

  def labelIndices() = {
    LabelIndex.findAll()
  }

  def labelMetas() = {
    LabelMeta.findAll()
  }

  def columnMetas() = {
    ColumnMeta.findAll()
  }
}

class S2Type(_repo: GraphRepository) {
  implicit val repo = _repo

  import S2Type._
  import org.apache.s2graph.graphql.bind.Unmarshaller._

  /**
    * fields
    */
  val serviceFields: List[Field[GraphRepository, Any]] = {

    repo.services.flatMap { service =>
      val ServiceType = makeServiceType(service)

      val f = Field(
        service.serviceName.toValidName,
        ServiceType,
        description = Some(s"serviceName: ${service.serviceName}"),
        resolve = _ => service
      ): Field[GraphRepository, Any]

      List(f)
    }
  }

  /**
    * arguments
    */
  lazy val addVertexArg = {
    val serviceArguments = repo.services.map { service =>
      val serviceFields = DummyInputField +: makeInputFieldsOnService(service)

      val ServiceInputType = InputObjectType[List[AddVertexParam]](
        s"Input_vertex_${service.serviceName.toValidName}_param",
        () => serviceFields.toList
      )
      Argument(service.serviceName.toValidName, OptionInputType(ServiceInputType))
    }

    serviceArguments
  }

  lazy val addEdgeArg = {
    val labelArguments = repo.labels.map { label =>
      val labelFields = DummyInputField +: makeInputFieldsOnLabel(label)
      val labelInputType = InputObjectType[AddEdgeParam](
        s"Input_label_${label.label.toValidName}_param",
        () => labelFields.toList
      )

      Argument(label.label.toValidName, OptionInputType(ListInputType(labelInputType)))
    }

    labelArguments
  }

  /**
    * Query fields
    * Provide s2graph query / mutate API
    * - Fields is created(or changed) for metadata is changed.
    */
  val queryFields = serviceFields

  lazy val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("addVertex",
      ListType(MutateResponseType),
      arguments = addVertexArg,
      resolve = c => {
        val vertices = c.args.raw.keys.flatMap { serviceName =>
          val addVertexParams = c.arg[List[AddVertexParam]](serviceName)
          addVertexParams.map { param =>
            repo.toS2VertexLike(serviceName, param)
          }
        }

        c.ctx.addVertices(vertices.toSeq)
      }
    ),
    Field("addEdge",
      ListType(MutateResponseType),
      arguments = addEdgeArg,
      resolve = c => {
        val edges = c.args.raw.keys.flatMap { labelName =>
          val addEdgeParams = c.arg[Vector[AddEdgeParam]](labelName)
          addEdgeParams.map { param =>
            repo.toS2EdgeLike(labelName, param)
          }
        }

        c.ctx.addEdges(edges.toSeq)
      }
    )
  )
}
