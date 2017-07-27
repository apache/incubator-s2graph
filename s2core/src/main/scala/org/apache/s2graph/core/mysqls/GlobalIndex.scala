package org.apache.s2graph.core.mysqls

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import scalikejdbc.{AutoSession, DBSession, WrappedResultSet}
import scalikejdbc._

object GlobalIndex extends Model[GlobalIndex] {
  import org.apache.s2graph.core.index.IndexProvider._

  val DefaultIndexName = GlobalIndex(None, Seq(vidField, eidField, serviceField, serviceColumnField, labelField), "_default_")

  val TableName = "global_indices"

  def apply(rs: WrappedResultSet): GlobalIndex = {
    GlobalIndex(rs.intOpt("id"), rs.string("prop_names").split(",").sorted, rs.string("index_name"))
  }

  def findBy(indexName: String, useCache: Boolean = true)(implicit session: DBSession = AutoSession): Option[GlobalIndex] = {
    val cacheKey = s"indexName=$indexName"
    lazy val sql = sql"""select * from global_indices where index_name = $indexName""".map { rs => GlobalIndex(rs) }.single.apply()
    if (useCache) withCache(cacheKey){sql}
    else sql
  }

  def insert(indexName: String, propNames: Seq[String])(implicit session: DBSession = AutoSession): Long = {
    sql"""insert into global_indices(prop_names, index_name) values(${propNames.sorted.mkString(",")}, $indexName)"""
      .updateAndReturnGeneratedKey.apply()
  }

  def findAll(useCache: Boolean = true)(implicit session: DBSession = AutoSession): Seq[GlobalIndex] = {
    lazy val ls = sql"""select * from global_indices """.map { rs => GlobalIndex(rs) }.list.apply
    if (useCache) {
      listCache.withCache("findAll") {
        putsToCache(ls.map { globalIndex =>
          val cacheKey = s"indexName=${globalIndex.indexName}"
          cacheKey -> globalIndex
        })
        ls
      }
    } else {
      ls
    }
  }

  def findGlobalIndex(hasContainers: java.util.List[HasContainer])(implicit session: DBSession = AutoSession): Option[GlobalIndex] = {
    import scala.collection.JavaConversions._
    val indices = findAll(useCache = true)
    val keys = hasContainers.map(_.getKey)

    val sorted = indices.map { index =>
      val matched = keys.filter(index.propNamesSet)
      index -> matched.length
    }.filter(_._2 > 0).sortBy(_._2 * -1)

    sorted.headOption.map(_._1)
  }

}

case class GlobalIndex(id: Option[Int], propNames: Seq[String], indexName: String)  {
  lazy val propNamesSet = propNames.toSet
}
