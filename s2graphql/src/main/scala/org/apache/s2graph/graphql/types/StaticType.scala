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
import org.apache.s2graph.core.{JSONParser, S2EdgeLike, S2VertexLike}
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.macros.derive._
import sangria.schema._

import scala.util.{Failure, Random, Success, Try}

object StaticTypes {
  val MutateResponseType = deriveObjectType[GraphRepository, MutateResponse](
    ObjectTypeName("MutateGraphElement"),
    ObjectTypeDescription("desc here"),
    AddFields(
      Field("isSuccess", BooleanType, resolve = c => c.value.isSuccess)
    )
  )

  val DataTypeType = EnumType(
    "Enum_DataType",
    description = Option("desc here"),
    values = List(
      EnumValue("string", value = "string"),
      EnumValue("int", value = "int"),
      EnumValue("long", value = "long"),
      EnumValue("double", value = "double"),
      EnumValue("boolean", value = "boolean")
    )
  )

  val InDirectionType = EnumType(
    "Enum_Direction_In",
    description = Option("desc here"),
    values = List(
      EnumValue("in", value = "in")
    )
  )

  val OutDirectionType = EnumType(
    "Enum_Direction_Out",
    description = Option("desc here"),
    values = List(
      EnumValue("out", value = "out")
    )
  )
  val BothDirectionType = EnumType(
    "Enum_Direction_Both",
    description = Option("desc here"),
    values = List(
      EnumValue("out", value = "out"),
      EnumValue("in", value = "in")
    )
  )

  val LabelMetaType = deriveObjectType[GraphRepository, LabelMeta](
    ObjectTypeName("LabelMeta"),
    ExcludeFields("seq", "labelId")
  )

  val ColumnMetaType = deriveObjectType[GraphRepository, ColumnMeta](
    ObjectTypeName("ColumnMeta"),
    ExcludeFields("seq", "columnId")
  )

  val InputIndexType = InputObjectType[Index](
    "Input_Index",
    description = "desc here",
    fields = List(
      InputField("name", StringType),
      InputField("propNames", ListInputType(StringType))
    )
  )

  val InputPropType = InputObjectType[Prop](
    "Input_Prop",
    description = "desc here",
    fields = List(
      InputField("name", StringType),
      InputField("dataType", DataTypeType),
      InputField("defaultValue", StringType),
      InputField("storeInGlobalIndex", BooleanType)
    )
  )

  val CompressionAlgorithmType = EnumType(
    "Enum_CompressionAlgorithm",
    description = Option("desc here"),
    values = List(
      EnumValue("gz", description = Option("desc here"), value = "gz"),
      EnumValue("lz4", description = Option("desc here"), value = "lz4")
    )
  )

  val ConsistencyLevelType = EnumType(
    "Enum_Consistency",
    description = Option("desc here"),
    values = List(
      EnumValue("weak", description = Option("desc here"), value = "weak"),
      EnumValue("strong", description = Option("desc here"), value = "strong")
    )
  )

  val LabelIndexType = deriveObjectType[GraphRepository, LabelIndex](
    ObjectTypeName("LabelIndex"),
    ObjectTypeDescription("desc here"),
    ExcludeFields("seq", "metaSeqs", "formulars", "labelId")
  )

  val LabelType = deriveObjectType[GraphRepository, Label](
    ObjectTypeName("Label"),
    ObjectTypeDescription("desc here"),
    AddFields(
      Field("indices", ListType(LabelIndexType), resolve = c => c.value.indices),
      Field("props", ListType(LabelMetaType), resolve = c => c.value.labelMetas)
    ),
    RenameField("label", "name")
  )

  val DummyInputField = InputField("_", OptionInputType(LongType))

  val DummyObjectTypeField: Field[GraphRepository, Any] = Field(
    "_",
    OptionType(LongType),
    description = Some("dummy field"),
    resolve = _ => None
  )
}
