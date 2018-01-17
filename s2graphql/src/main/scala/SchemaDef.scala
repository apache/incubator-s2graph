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

/**
  * S2Graph GraphQL schema.
  *
  * When a Label or Service is created, the GraphQL schema is created dynamically.
  */
class SchemaDef(s2Type: S2Type) {

  import sangria.schema._

  val S2QueryType = ObjectType[GraphRepository, Any]("Query", fields(s2Type.queryFields: _*))

  val S2MutationType = ObjectType[GraphRepository, Any]("Mutation", fields(s2Type.mutationFields: _*))

  val S2GraphSchema = Schema(S2QueryType, Option(S2MutationType))
}
