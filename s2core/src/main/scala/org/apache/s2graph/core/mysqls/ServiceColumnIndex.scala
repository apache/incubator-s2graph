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

import scalikejdbc.{AutoSession, DBSession, WrappedResultSet, _}

object ServiceColumnIndex extends Model[ServiceColumnIndex] {
  val dbTableName = "service_column_indices"
  val DefaultName = "_PK"
  val DefaultSeq = 1.toByte
  val MaxOrderSeq = 7

  def apply(rs: WrappedResultSet): ServiceColumnIndex = {
    ServiceColumnIndex(rs.intOpt("id"), rs.int("service_id"), rs.int("service_column_id"),
      rs.string("name"),
      rs.byte("seq"), rs.string("meta_seqs").split(",").filter(_ != "").map(s => s.toByte).toList match {
        case metaSeqsList => metaSeqsList
      },
      rs.stringOpt("options")
    )
  }

  def findById(id: Int)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "id=" + id
    lazy val sql = sql"""select * from $dbTableName where id = ${id}"""
    withCache(cacheKey) {
      sql.map { rs => ServiceColumnIndex(rs) }.single.apply
    }.get
  }

  def findBySeqs(serviceId: Int, serviceColumnId: Int, seqs: List[Byte])(implicit session: DBSession = AutoSession): Option[ServiceColumnIndex] = {
    val cacheKey = "serviceId=" + serviceId + ":serviceColumnId=" + serviceColumnId + ":seqs=" + seqs.mkString(",")
    lazy val sql =
      sql"""
      select * from $dbTableName where service_id = $serviceId and service_column_id = $serviceColumnId and meta_seqs = ${seqs.mkString(",")}
      """
    withCache(cacheKey) {
      sql.map { rs => ServiceColumnIndex(rs) }.single.apply
    }
  }

  def findBySeq(serviceId: Int,
                serviceColumnId: Int,
                seq: Byte,
                useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    val cacheKey = "serviceId=" + serviceId + ":serviceColumnId=" + serviceColumnId + ":seq=" + seq
    lazy val sql =
      sql"""
      select * from $dbTableName where service_id = $serviceId and service_column_id = $serviceColumnId and seq = ${seq}
      """
    if (useCache) {
      withCache(cacheKey)(sql.map { rs => ServiceColumnIndex(rs) }.single.apply)
    } else {
      sql.map { rs => ServiceColumnIndex(rs) }.single.apply
    }
  }


  def findAll(serviceId: Int, serviceColumnId: Int, useCache: Boolean = true)(implicit session: DBSession = AutoSession) = {
    val cacheKey = s"serviceId=$serviceId:serviceColumnId=$serviceColumnId"
    lazy val sql =
      sql"""
          select * from $dbTableName where service_id = ${serviceId} and seq > 0 order by seq ASC
        """
    if (useCache) {
      withCaches(cacheKey)(
        sql.map { rs => ServiceColumnIndex(rs) }.list.apply
      )
    } else {
      sql.map { rs => LabelIndex(rs) }.list.apply
    }
  }

  def insert(serviceId: Int,
             serviceColumnId: Int,
             indexName: String,
             seq: Byte, metaSeqs: List[Byte], options: Option[String])(implicit session: DBSession = AutoSession): Long = {
    sql"""
    	insert into $dbTableName(service_id, service_column_id, name, seq, meta_seqs, options)
    	values (${serviceId}, ${serviceColumnId}, ${indexName}, ${seq}, ${metaSeqs.mkString(",")}, ${options})
    """
      .updateAndReturnGeneratedKey.apply()
  }

  def findOrInsert(serviceId: Int,
                   serviceColumnId: Int,
                   indexName: String,
                   metaSeqs: List[Byte],
                   options: Option[String])(implicit session: DBSession = AutoSession): ServiceColumnIndex = {
    findBySeqs(serviceId, serviceColumnId, metaSeqs) match {
      case Some(s) => s
      case None =>
        val orders = findAll(serviceId, serviceColumnId, false)
        val seq = (orders.size + 1).toByte
        assert(seq <= MaxOrderSeq)
        val createdId = insert(serviceId, serviceColumnId, indexName, seq, metaSeqs, options)
        val cacheKeys = toCacheKeys(createdId.toInt, serviceId, serviceColumnId, seq, metaSeqs)

        cacheKeys.foreach { key =>
          expireCache(key)
          expireCaches(key)
        }
        findBySeq(serviceId, serviceColumnId, seq).get
    }
  }

  def toCacheKeys(id: Int, serviceId: Int, serviceColumnId: Int, seq: Byte, seqs: Seq[Byte]): Seq[String] = {
    Seq(s"id=$id",
      s"serviceId=$serviceId:serviceColumnId=$serviceColumnId:seq=$seq",
      s"serviceId=$serviceId:serviceColumnId=$serviceColumnId:seqs=$seqs",
      s"serviceId=$serviceId:serviceColumnId=$serviceColumnId")
  }

  def delete(id: Int)(implicit session: DBSession = AutoSession) = {
    val me = findById(id)
    val seqs = me.metaSeqs.mkString(",")
    val (serviceId, serviceColumnId, seq) = (me.serviceId, me.serviceColumnId, me.seq)
    lazy val sql = sql"""delete from $dbTableName where id = ${id}"""

    sql.execute.apply()

    val cacheKeys = toCacheKeys(id, serviceId, serviceColumnId, seq, me.metaSeqs)

    cacheKeys.foreach { key =>
      expireCache(key)
      expireCaches(key)
    }
  }

  def findAll()(implicit session: DBSession = AutoSession) = {
    val ls = sql"""select * from $dbTableName""".map { rs => ServiceColumnIndex(rs) }.list.apply
    val singles = ls.flatMap { x =>
      val cacheKeys = toCacheKeys(x.id.get, x.serviceId, x.serviceColumnId, x.seq, x.metaSeqs).dropRight(1)
      cacheKeys.map { cacheKey =>
        cacheKey -> x
      }
    }
    val multies = ls.groupBy(x => (x.serviceId, x.serviceColumnId)).map { case ((serviceId, serviceColumnId), ls) =>
      val cacheKey = s"serviceId=$serviceId:serviceColumnId=$serviceColumnId"
      cacheKey -> ls
    }.toList

    putsToCache(singles)
    putsToCaches(multies)

  }
}

case class ServiceColumnIndex(id: Option[Int],
                              serviceId: Int,
                              serviceColumnId: Int,
                              name: String,
                              seq: Byte,
                              metaSeqs: Seq[Byte],
                              options: Option[String]) {

}
