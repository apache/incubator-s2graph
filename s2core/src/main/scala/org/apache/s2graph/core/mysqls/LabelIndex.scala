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

import org.apache.s2graph.core.GraphUtil
import org.apache.s2graph.core.mysqls.LabelIndex.WriteOption
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.{JsObject, JsString, Json}
import scalikejdbc._

object LabelIndex extends Model[LabelIndex] {
  val DefaultName = "_PK"
  val DefaultMetaSeqs = Seq(LabelMeta.timestampSeq)
  val DefaultSeq = 1.toByte
  val MaxOrderSeq = 7

  def apply(rs: WrappedResultSet): LabelIndex = {
    LabelIndex(rs.intOpt("id"), rs.int("label_id"), rs.string("name"), rs.byte("seq"),
      rs.string("meta_seqs").split(",").filter(_ != "").map(s => s.toByte).toList match {
        case metaSeqsList => metaSeqsList
      },
      rs.string("formulars"),
      rs.intOpt("dir"),
      rs.stringOpt("options")
    )
  }

  case class WriteOption(dir: Byte,
                         method: String,
                         rate: Double,
                         totalModular: Long,
                         storeDegree: Boolean) {

    val isBufferIncrement = method == "drop" || method == "sample" || method == "hash_sample"

    def sample[T](a: T, hashOpt: Option[Long]): Boolean = {
      if (method == "drop") false
      else if (method == "sample") {
        if (scala.util.Random.nextDouble() < rate) true
        else false
      } else if (method == "hash_sample") {
        val hash = hashOpt.getOrElse(throw new RuntimeException("hash_sample need _from_hash value"))
        if ((hash.abs % totalModular) / totalModular.toDouble < rate) true
        else false
      } else true
    }
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "id=" + id
    withCache(cacheKey) {
      sql"""select * from label_indices where id = ${id}""".map { rs => LabelIndex(rs) }.single.apply
    }.get
  }

  def findByLabelIdAll(labelId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "labelId=" + labelId
    if (useCache) {
      withCaches(cacheKey)( sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0 order by seq ASC
      """.map { rs => LabelIndex(rs) }.list.apply)
    } else {
      sql"""
        select * from label_indices where label_id = ${labelId} and seq > 0 order by seq ASC
      """.map { rs => LabelIndex(rs) }.list.apply
    }
  }

  def insert(labelId: Int, indexName: String, seq: Byte, metaSeqs: List[Byte], formulars: String,
             direction: Option[Int], options: Option[String])(implicit session: DBSession = AutoSession): Long = {
    sql"""
    	insert into label_indices(label_id, name, seq, meta_seqs, formulars, dir, options)
    	values (${labelId}, ${indexName}, ${seq}, ${metaSeqs.mkString(",")}, ${formulars}, ${direction}, ${options})
    """
      .updateAndReturnGeneratedKey.apply()
  }

  def findOrInsert(labelId: Int, indexName: String, metaSeqs: List[Byte], formulars: String,
                   direction: Option[Int], options: Option[String])(implicit session: DBSession = AutoSession): LabelIndex = {
    findByLabelIdAndSeqs(labelId, metaSeqs, direction) match {
      case Some(s) => s
      case None =>
        val orders = findByLabelIdAll(labelId, false)
        val seq = (orders.size + 1).toByte
        assert(seq <= MaxOrderSeq)
        val createdId = insert(labelId, indexName, seq, metaSeqs, formulars, direction, options)
        val cacheKeys = List(s"labelId=$labelId:seq=$seq",
          s"labelId=$labelId:seqs=$metaSeqs:dir=$direction", s"labelId=$labelId:seq=$seq", s"id=$createdId")

        cacheKeys.foreach { key =>
          expireCache(key)
          expireCaches(key)
        }

        findByLabelIdAndSeq(labelId, seq).get
    }
  }

  def findByLabelIdAndSeqs(labelId: Int, seqs: List[Byte], direction: Option[Int])(implicit session: DBSession = AutoSession): Option[LabelIndex] = {
    val cacheKey = "labelId=" + labelId + ":seqs=" + seqs.mkString(",") + ":dir=" + direction
    withCache(cacheKey) {
      sql"""
      select * from label_indices where label_id = ${labelId} and meta_seqs = ${seqs.mkString(",")} and dir = ${direction}
      """.map { rs => LabelIndex(rs) }.single.apply
    }
  }

  def findByLabelIdAndSeq(labelId: Int, seq: Byte, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[LabelIndex] = {
    //    val cacheKey = s"labelId=$labelId:seq=$seq"
    val cacheKey = "labelId=" + labelId + ":seq=" + seq
    if (useCache) {
      withCache(cacheKey)( sql"""
      select * from label_indices where label_id = ${labelId} and seq = ${seq}
      """.map { rs => LabelIndex(rs) }.single.apply)
    } else {
      sql"""
      select * from label_indices where label_id = ${labelId} and seq = ${seq}
      """.map { rs => LabelIndex(rs) }.single.apply
    }
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val labelIndex = findById(id)
    val seqs = labelIndex.metaSeqs.mkString(",")
    val (labelId, seq) = (labelIndex.labelId, labelIndex.seq)
    sql"""delete from label_indices where id = ${id}""".execute.apply()

    val cacheKeys = List(s"id=$id", s"labelId=$labelId", s"labelId=$labelId:seq=$seq", s"labelId=$labelId:seqs=$seqs:dir=${labelIndex.dir}")
    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from label_indices""".map { rs => LabelIndex(rs) }.list.apply
    putsToCache(ls.map { x =>
      var cacheKey = s"id=${x.id.get}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"labelId=${x.labelId}:seq=${x.seq}}"
      (cacheKey -> x)
    })
    putsToCache(ls.map { x =>
      var cacheKey = s"labelId=${x.labelId}:seqs=${x.metaSeqs.mkString(",")}:dir=${x.dir}"
      (cacheKey -> x)
    })
    putsToCaches(ls.groupBy(x => x.labelId).map { case (labelId, ls) =>
      val cacheKey = s"labelId=${labelId}"
      (cacheKey -> ls)
    }.toList)
  }
}

case class LabelIndex(id: Option[Int], labelId: Int, name: String, seq: Byte, metaSeqs: Seq[Byte], formulars: String,
                      dir: Option[Int], options: Option[String]) {
  // both
  lazy val label = Label.findById(labelId)
  lazy val metas = label.metaPropsMap
  lazy val sortKeyTypes = metaSeqs.flatMap(metaSeq => label.metaPropsMap.get(metaSeq))
  lazy val sortKeyTypesArray = sortKeyTypes.toArray
  lazy val propNames = sortKeyTypes.map { labelMeta => labelMeta.name }

  lazy val toJson = {
    val dirJs = dir.map(GraphUtil.fromDirection).getOrElse("both")
    val optionsJs = try { options.map(Json.parse).getOrElse(Json.obj()) } catch { case e: Exception => Json.obj() }

    Json.obj(
      "name" -> name,
      "propNames" -> sortKeyTypes.map(x => x.name),
      "dir" -> dirJs,
      "options" -> optionsJs
    )
  }

  def parseOption(dir: String): Option[WriteOption] = try {
    options.map { string =>
      val jsObj = Json.parse(string) \ dir

      val method = (jsObj \ "method").asOpt[String].getOrElse("default")
      val rate = (jsObj \ "rate").asOpt[Double].getOrElse(1.0)
      val totalModular = (jsObj \ "totalModular").asOpt[Long].getOrElse(100L)
      val storeDegree = (jsObj \ "storeDegree").asOpt[Boolean].getOrElse(true)

      WriteOption(GraphUtil.directions(dir).toByte, method, rate, totalModular, storeDegree)
    }
  } catch {
    case e: Exception =>
      logger.error(s"Parse failed labelOption: ${this.label}", e)
      None
  }

  lazy val inDirOption = parseOption("in")

  lazy val outDirOption = parseOption("out")
}
