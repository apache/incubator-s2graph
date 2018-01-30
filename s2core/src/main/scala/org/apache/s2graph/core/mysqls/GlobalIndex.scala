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

package org.apache.s2graph.core.mysqls

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import scalikejdbc.{AutoSession, DBSession, WrappedResultSet}
import scalikejdbc._

object GlobalIndex extends Model[GlobalIndex] {
  val vidField = "_vid_"
  val eidField = "_eid_"
  val labelField = "_label_"
  val serviceField = "_service_"
  val serviceColumnField = "_serviceColumn_"
  val EdgeType = "edge"
  val VertexType = "vertex"
  val hiddenIndexFields = Set(vidField, eidField, labelField, serviceField, serviceColumnField)

  val TableName = "global_indices"

  def apply(rs: WrappedResultSet): GlobalIndex = {
    GlobalIndex(rs.intOpt("id"),
      rs.string("element_type"),
      rs.string("prop_names").split(",").sorted,
      rs.string("index_name"))
  }

  def findBy(elementType: String, indexName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[GlobalIndex] = {
    val cacheKey = s"elementType=$elementType:indexName=$indexName"
    lazy val sql = sql"""select * from global_indices where element_type = ${elementType} and index_name = $indexName""".map { rs => GlobalIndex(rs) }.single.apply()

    if (useCache) withCache(cacheKey){sql}
    else sql
  }

  def insert(elementType: String, indexName: String, propNames: Seq[String])(implicit session: DBSession = AutoSession): Long = {
    val allPropNames = (hiddenIndexFields.toSeq ++ propNames).sorted
    sql"""insert into global_indices(element_type, prop_names, index_name)
         values($elementType, ${allPropNames.mkString(",")}, $indexName)"""
      .updateAndReturnGeneratedKey.apply()
  }

  def findAll(elementType: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Seq[GlobalIndex] = {
    lazy val ls = sql"""select * from global_indices where element_type = $elementType""".map { rs => GlobalIndex(rs) }.list.apply
    if (useCache) {
      listCache.withCache(s"findAll:elementType=$elementType") {
        putsToCache(ls.map { globalIndex =>
          val cacheKey = s"elementType=${globalIndex.elementType}:indexName=${globalIndex.indexName}"
          cacheKey -> globalIndex
        })
        ls
      }
    } else {
      ls
    }
  }

  def findGlobalIndex(elementType: String, hasContainers: java.util.List[HasContainer])(implicit session: DBSession = AutoSession): Option[GlobalIndex] = {
    import scala.collection.JavaConversions._
    val indices = findAll(elementType, useCache = true)
    val keys = hasContainers.map(_.getKey)

    val sorted = indices.map { index =>
      val matched = keys.filter(index.propNamesSet)
      index -> matched.length
    }.filter(_._2 > 0).sortBy(_._2 * -1)

    sorted.headOption.map(_._1)
  }

}

case class GlobalIndex(id: Option[Int],
                       elementType: String,
                       propNames: Seq[String],
                       indexName: String)  {
  val backendIndexName = indexName + "_" + elementType
  val backendIndexNameWithType = backendIndexName + "/test1"
  lazy val propNamesSet = propNames.toSet
}
