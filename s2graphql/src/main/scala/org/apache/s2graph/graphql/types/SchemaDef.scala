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

import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema.ObjectType

import scala.collection.mutable

/**
  * S2Graph GraphQL schema.
  *
  * When a Label or Service is created, the GraphQL schema is created dynamically.
  */
class SchemaDef(g: GraphRepository, withAdmin: Boolean = false) {

  import sangria.schema._

  val s2Type = new S2Type(g)
  val s2ManagementType = new ManagementType(g)

  val queryManagementFields = List(wrapField("QueryManagement", "Management", s2ManagementType.queryFields))
  val S2QueryType = ObjectType[GraphRepository, Any](
    "Query",
    fields(s2Type.queryFields ++ queryManagementFields: _*)
  )

  lazy val mutateManagementFields = List(wrapField("MutationManagement", "Management", s2ManagementType.mutationFields))

  val S2MutationType =
    if (!withAdmin) None
    else {
      val mutationTpe = ObjectType[GraphRepository, Any](
        "Mutation",
        fields(s2Type.mutationFields ++ mutateManagementFields: _*)
      )

      Option(mutationTpe)
    }

  val directives = S2Directive.Transform :: BuiltinDirectives

  private val s2Schema = Schema(
    S2QueryType,
    S2MutationType,
    directives = directives
  )

  val schema = s2Schema
}
