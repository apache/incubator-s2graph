package org.apache.s2graph.core
import java.util
import java.util.function.{BiConsumer, Consumer}

import org.apache.s2graph.core.S2Vertex.Props
import org.apache.s2graph.core.mysqls.{ColumnMeta, Label, Service, ServiceColumn}
import org.apache.s2graph.core.types.VertexId
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, T, Vertex, VertexProperty}
import play.api.libs.json.Json

import scala.concurrent.{Await, Future}
import scala.collection.JavaConverters._

trait S2VertexLike extends Vertex with GraphElement {
  this: S2Vertex =>

  val graph: S2Graph
  val id: VertexId
  val ts: Long
  val props: Props
  val op: Byte
  val belongLabelIds: Seq[Int]

  val innerId = id.innerId
  val innerIdVal = innerId.value


  lazy val properties = for {
    (k, v) <- props.asScala
  } yield v.columnMeta.name -> v.value

  def schemaVer = serviceColumn.schemaVersion

  def serviceColumn = ServiceColumn.findById(id.colId)

  def columnName = serviceColumn.columnName

  lazy val service = Service.findById(serviceColumn.serviceId)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.hTableName)

  def defaultProps = {
    val default = S2Vertex.EmptyProps
    val newProps = new S2VertexProperty(this, ColumnMeta.lastModifiedAtColumn, ColumnMeta.lastModifiedAtColumn.name, ts)
    default.put(ColumnMeta.lastModifiedAtColumn.name, newProps)
    default
  }

  def propsWithName = for {
    (k, v) <- props.asScala
  } yield (v.columnMeta.name -> v.value.toString)

  def toLogString(): String = {
    val (serviceName, columnName) =
      if (!id.storeColId) ("", "")
      else (serviceColumn.service.serviceName, serviceColumn.columnName)

    if (propsWithName.nonEmpty)
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName, Json.toJson(propsWithName)).mkString("\t")
    else
      Seq(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName).mkString("\t")
  }

  def copyVertexWithState(props: Props): S2VertexLike = {
    val newVertex = copy(props = S2Vertex.EmptyProps)
    S2Vertex.fillPropsWithTs(newVertex, props)
    newVertex
  }

  def vertices(direction: Direction, edgeLabels: String*): util.Iterator[Vertex] = {
    val arr = new util.ArrayList[Vertex]()
    edges(direction, edgeLabels: _*).forEachRemaining(new Consumer[Edge] {
      override def accept(edge: Edge): Unit = {
        val s2Edge = edge.asInstanceOf[S2Edge]

        s2Edge.direction match {
          case "out" => arr.add(edge.inVertex())
          case "in" => arr.add(edge.outVertex())
          case _ => throw new IllegalStateException("only out/in direction can be found in S2Edge")
        }
      }
    })
    arr.iterator()
  }

  def edges(direction: Direction, labelNames: String*): util.Iterator[Edge] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    graph.fetchEdges(this, labelNameWithDirs.distinct)
  }

  // do no save to storage
  def propertyInner[V](cardinality: Cardinality, key: String, value: V, objects: AnyRef*): VertexProperty[V] = {
    S2Property.assertValidProp(key, value)

    cardinality match {
      case Cardinality.single =>
        val columnMeta = serviceColumn.metasInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Vertex."))
        val newProps = new S2VertexProperty[V](this, columnMeta, key, value)
        props.put(key, newProps)
        newProps
      case _ => throw new RuntimeException("only single cardinality is supported.")
    }
  }

  def property[V](cardinality: Cardinality, key: String, value: V, objects: AnyRef*): VertexProperty[V] = {
    S2Property.assertValidProp(key, value)

    cardinality match {
      case Cardinality.single =>
        val columnMeta = serviceColumn.metasInvMap.getOrElse(key, throw new RuntimeException(s"$key is not configured on Vertex."))
        val newProps = new S2VertexProperty[V](this, columnMeta, key, value)
        props.put(key, newProps)

        // FIXME: save to persistent for tp test
        graph.addVertexInner(this)
        newProps
      case _ => throw new RuntimeException("only single cardinality is supported.")
    }
  }

  def addEdge(labelName: String, vertex: Vertex, kvs: AnyRef*): Edge = {
    graph.addEdge(this, labelName, vertex, kvs: _*)
  }

  def property[V](key: String): VertexProperty[V] = {
    if (props.containsKey(key)) {
      props.get(key).asInstanceOf[S2VertexProperty[V]]
    } else {
      VertexProperty.empty()
    }
  }

  def properties[V](keys: String*): util.Iterator[VertexProperty[V]] = {
    val ls = new util.ArrayList[VertexProperty[V]]()
    if (keys.isEmpty) {
      props.forEach(new BiConsumer[String, VertexProperty[_]] {
        override def accept(key: String, property: VertexProperty[_]): Unit = {
          if (!ColumnMeta.reservedMetaNamesSet(key) && property.isPresent && key != T.id.name)
            ls.add(property.asInstanceOf[VertexProperty[V]])
        }
      })
    } else {
      keys.foreach { key =>
        val prop = property[V](key)
        if (prop.isPresent) ls.add(prop)
      }
    }
    ls.iterator
  }

  def label(): String = {
    serviceColumn.columnName
  }

  def remove(): Unit = {
    if (graph.features().vertex().supportsRemoveVertices()) {
      // remove edge
      // TODO: remove related edges also.
      implicit val ec = graph.ec

      val verticesToDelete = Seq(this.copy(op = GraphUtil.operations("delete")))

      val vertexFuture = graph.mutateVertices(verticesToDelete, withWait = true)

      val future = for {
        vertexSuccess <- vertexFuture
        edges <- edgesAsync(Direction.BOTH)
      } yield {
        edges.asScala.toSeq.foreach { edge => edge.remove() }
        if (!vertexSuccess.forall(_.isSuccess)) throw new RuntimeException("Vertex.remove vertex delete failed.")

        true
      }
      Await.result(future, graph.WaitTimeout)

    } else {
      throw Vertex.Exceptions.vertexRemovalNotSupported()
    }
  }

  private def edgesAsync(direction: Direction, labelNames: String*): Future[util.Iterator[Edge]] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    graph.fetchEdgesAsync(this, labelNameWithDirs.distinct)
  }
}
