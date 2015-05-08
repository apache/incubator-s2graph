package com.daumkakao.s2graph.core
import scalikejdbc._

object ColumnMeta extends LocalCache[ColumnMeta] {

  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val lastModifiedAtColumnSeq = 0.toByte
  val lastModifiedAtColumn = ColumnMeta(Some(0), 0, "lastModifiedAt", lastModifiedAtColumnSeq)
  val maxValue = Byte.MaxValue
  
  def apply(rs: WrappedResultSet): ColumnMeta = {
    ColumnMeta(Some(rs.int("id")), rs.int("column_id"), rs.string("name"), rs.byte("seq"))
  }

  def findById(id: Int) = {
    val cacheKey = s"id=$id"
    withCache(cacheKey) {
      sql"""select * from column_metas where id = ${id}""".map { rs => ColumnMeta(rs) }.single.apply
    }.get
  }
  def findAllByColumn(columnId: Int, useCache: Boolean = true) = {
    val cacheKey = s"columnId=$columnId"
    if (useCache) {
      withCaches(cacheKey)(sql"""select id, column_id, name, seq
    		  						from column_metas 
    		  						where column_id = ${columnId} order by seq ASC"""
        .map { rs => ColumnMeta(rs) }.list.apply())
    } else {
      sql"""select id, column_id, name, seq
      		from column_metas 
      		where column_id = ${columnId} order by seq ASC"""
        .map { rs => ColumnMeta(rs) }.list.apply()
    }
  }
  def findByName(columnId: Int, name: String) = {
    val cacheKey = s"columnId=$columnId:name=$name"
    withCache(cacheKey)(sql"""
        select id, column_id, name, seq
        from column_metas where column_id = ${columnId} and name = ${name}"""
      .map { rs => ColumnMeta(rs) }.single.apply())
  }
  def insert(columnId: Int, name: String) = {
    val ls = findAllByColumn(columnId, false)
    val seq = ls.size + 1
    if (seq <= maxValue) {
      sql"""insert into column_metas(column_id, name, seq) 
    select ${columnId}, ${name}, ${seq}"""
        .updateAndReturnGeneratedKey.apply()
    }
  }

  def findOrInsert(columnId: Int, name: String): ColumnMeta = {
//    play.api.Logger.debug(s"findOrInsert: $columnId, $name")
    findByName(columnId, name) match {
      case Some(c) => c
      case None =>
        insert(columnId, name)
        expireCache(s"columnId=$columnId:name=$name")
        findByName(columnId, name).get
    }
  }
  def findByIdAndSeq(columnId: Int, seq: Byte) = {
    val cacheKey = s"columnId=$columnId:seq=$seq"
    withCache(cacheKey)(sql"""
        select * from column_metas where column_id = ${columnId} and seq = ${seq}
    """.map { rs => ColumnMeta(rs) }.single.apply())
  }
  def delete(id: Int) = {
    val columnMeta = findById(id)
    val (columnId, name) = (columnMeta.columnId, columnMeta.name)
    sql"""delete from column_metas where id = ${id}""".execute.apply()
    val cacheKeys = List(s"id=$id", s"columnId=$columnId:name=$name", s"colunmId=$columnId")
    cacheKeys.foreach(expireCache(_))
  }
}

case class ColumnMeta(id: Option[Int], columnId: Int, name: String, seq: Byte) 