package org.apache.s2graph.core.index

import java.util

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.typesafe.config.Config
import org.apache.s2graph.core.io.Conversions
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.VertexId
import org.apache.s2graph.core.{EdgeId, S2EdgeLike, S2VertexLike}
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class ESIndexProvider(config: Config)(implicit ec: ExecutionContext) extends IndexProvider {
  import GlobalIndex._
  import IndexProvider._

  import scala.collection.mutable

  implicit val executor = ec

  val esClientUri = "localhost"
//  //    config.getString("es.index.provider.client.uri")
  val client = HttpClient(ElasticsearchClientUri(esClientUri, 9200))
//  val node = LocalNode("elasticsearch", "./es")
//  val client = node.http(true)

  val WaitTime = Duration("60 seconds")

  private def toFields(globalIndex: GlobalIndex, vertex: S2VertexLike): Option[Map[String, Any]] = {
    val props = vertex.props.asScala
    val filtered = props.filterKeys(globalIndex.propNamesSet)

    if (filtered.isEmpty) None
    else {
      val fields = mutable.Map.empty[String, Any]

      fields += (vidField -> vertex.id.toString())
      fields += (serviceField -> vertex.serviceName)
      fields += (serviceColumnField -> vertex.columnName)

      filtered.foreach { case (dim, s2VertexProperty) =>
        s2VertexProperty.columnMeta.dataType match {
          case "string" => fields += (dim -> s2VertexProperty.innerVal.value.toString)
          case _ => fields += (dim -> s2VertexProperty.innerVal.value)
        }
      }

      Option(fields.toMap)
    }
  }

  private def toFields(globalIndex: GlobalIndex, edge: S2EdgeLike): Option[Map[String, Any]] = {
    val props = edge.getPropsWithTs().asScala
    val filtered = props.filterKeys(globalIndex.propNamesSet)

    if (filtered.isEmpty) None
    else {
      val fields = mutable.Map.empty[String, Any]

      fields += (eidField -> edge.edgeId.toString)
      fields += (serviceField -> edge.serviceName)
      fields += (labelField -> edge.label())

      filtered.foreach { case (dim, s2Property) =>
        s2Property.labelMeta.dataType match {
          case "string" => fields += (dim -> s2Property.innerVal.value.toString)
          case _ => fields += (dim -> s2Property.innerVal.value)
        }
      }

      Option(fields.toMap)
    }
  }

  override def mutateVerticesAsync(vertices: Seq[S2VertexLike]): Future[Seq[Boolean]] = {
    val globalIndexOptions = GlobalIndex.findAll(GlobalIndex.VertexType)

    val bulkRequests = globalIndexOptions.flatMap { globalIndex =>
      vertices.flatMap { vertex =>
        toFields(globalIndex, vertex).toSeq.map { fields =>
          indexInto(globalIndex.backendIndexNameWithType).fields(fields)
        }
      }
    }
    if (bulkRequests.isEmpty) Future.successful(vertices.map(_ => true))
    else {
      client.execute {
        val requests = bulk(requests = bulkRequests)

        requests
      }.map { ret =>
        ret match {
          case Left(failure) => vertices.map(_ => false)
          case Right(results) => vertices.map(_ => true)
        }
      }
    }
  }

  override def mutateVertices(vertices: Seq[S2VertexLike]): Seq[Boolean] =
    Await.result(mutateVerticesAsync(vertices), WaitTime)

  override def mutateEdges(edges: Seq[S2EdgeLike]): Seq[Boolean] =
    Await.result(mutateEdgesAsync(edges), WaitTime)

  override def mutateEdgesAsync(edges: Seq[S2EdgeLike]): Future[Seq[Boolean]] = {
    val globalIndexOptions = GlobalIndex.findAll(GlobalIndex.EdgeType)

    val bulkRequests = globalIndexOptions.flatMap { globalIndex =>
      edges.flatMap { edge =>
        toFields(globalIndex, edge).toSeq.map { fields =>
          indexInto(globalIndex.backendIndexNameWithType).fields(fields)
        }
      }
    }

    if (bulkRequests.isEmpty) Future.successful(edges.map(_ => true))
    else {
      client.execute {
        bulk(bulkRequests)
      }.map { ret =>
        ret match {
          case Left(failure) => edges.map(_ => false)
          case Right(results) => edges.map(_ => true)
        }
      }
    }
  }

  override def fetchEdgeIds(hasContainers: util.List[HasContainer]): util.List[EdgeId] =
    Await.result(fetchEdgeIdsAsync(hasContainers), WaitTime)

  override def fetchEdgeIdsAsync(hasContainers: util.List[HasContainer]): Future[util.List[EdgeId]] = {
    val field = eidField
    val ids = new java.util.HashSet[EdgeId]

    GlobalIndex.findGlobalIndex(GlobalIndex.EdgeType, hasContainers) match {
      case None => Future.successful(new util.ArrayList[EdgeId](ids))
      case Some(globalIndex) =>
        val queryString = buildQueryString(hasContainers)

        client.execute {
          search(globalIndex.backendIndexName).query(queryString)
        }.map { ret =>
          ret match {
            case Left(failure) =>
            case Right(results) =>
              results.result.hits.hits.foreach { searchHit =>
                searchHit.sourceAsMap.get(field).foreach { idValue =>
                  val id = Conversions.s2EdgeIdReads.reads(Json.parse(idValue.toString)).get

                  //TODO: Come up with better way to filter out hits with invalid meta.
                  EdgeId.isValid(id).foreach(ids.add)
                }
              }
          }

          new util.ArrayList[EdgeId](ids)
        }
    }
  }


  override def fetchVertexIds(hasContainers: util.List[HasContainer]): util.List[VertexId] =
    Await.result(fetchVertexIdsAsync(hasContainers), WaitTime)

  override def fetchVertexIdsAsync(hasContainers: util.List[HasContainer]): Future[util.List[VertexId]] = {
    val field = vidField
    val ids = new java.util.HashSet[VertexId]

    GlobalIndex.findGlobalIndex(GlobalIndex.VertexType, hasContainers) match {
      case None => Future.successful(new util.ArrayList[VertexId](ids))
      case Some(globalIndex) =>
        val queryString = buildQueryString(hasContainers)

        client.execute {
          search(globalIndex.backendIndexName).query(queryString)
        }.map { ret =>
          ret match {
            case Left(failure) =>
            case Right(results) =>
              results.result.hits.hits.foreach { searchHit =>
                searchHit.sourceAsMap.get(field).foreach { idValue =>
                  val id = Conversions.s2VertexIdReads.reads(Json.parse(idValue.toString)).get
                  //TODO: Come up with better way to filter out hits with invalid meta.
                  VertexId.isValid(id).foreach(ids.add)
                }
              }
          }

          new util.ArrayList[VertexId](ids)
        }
    }
  }

  override def shutdown(): Unit = {
    client.close()
  }
}
