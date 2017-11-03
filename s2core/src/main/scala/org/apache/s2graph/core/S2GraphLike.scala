package org.apache.s2graph.core
import java.util

import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.GraphExceptions.LabelNotExistException
import org.apache.s2graph.core.S2Graph.{DefaultColumnName, DefaultServiceName}
import org.apache.s2graph.core.features.{S2Features, S2GraphVariables}
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.types.VertexId
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure
import org.apache.tinkerpop.gremlin.structure.Edge.Exceptions
import org.apache.tinkerpop.gremlin.structure.Graph.{Features, Variables}
import org.apache.tinkerpop.gremlin.structure.io.{GraphReader, GraphWriter, Io, Mapper}
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Graph, T, Transaction, Vertex}

import scala.concurrent.{Await, Future}
import scala.collection.JavaConversions._

trait S2GraphLike extends Graph {
  this: S2Graph =>

  var apacheConfiguration: Configuration

  private val s2Features = new S2Features

  override def features() = s2Features

  def vertices(ids: AnyRef*): util.Iterator[structure.Vertex] = {
    val fetchVertices = ids.lastOption.map { lastParam =>
      if (lastParam.isInstanceOf[Boolean]) lastParam.asInstanceOf[Boolean]
      else true
    }.getOrElse(true)

    if (ids.isEmpty) {
      //TODO: default storage need to be fixed.
      Await.result(defaultStorage.fetchVerticesAll(), WaitTimeout).iterator
    } else {
      val vertices = ids.collect {
        case s2Vertex: S2VertexLike => s2Vertex
        case vId: VertexId => newVertex(vId)
        case vertex: Vertex => newVertex(vertex.id().asInstanceOf[VertexId])
        case other@_ => newVertex(VertexId.fromString(other.toString))
      }

      if (fetchVertices) {
        val future = getVertices(vertices).map { vs =>
          val ls = new util.ArrayList[structure.Vertex]()
          ls.addAll(vs)
          ls.iterator()
        }
        Await.result(future, WaitTimeout)
      } else {
        vertices.iterator
      }
    }
  }

  def edges(edgeIds: AnyRef*): util.Iterator[structure.Edge] = {
    if (edgeIds.isEmpty) {
      // FIXME
      Await.result(defaultStorage.fetchEdgesAll(), WaitTimeout).iterator
    } else {
      Await.result(edgesAsync(edgeIds: _*), WaitTimeout)
    }
  }

  def edgesAsync(edgeIds: AnyRef*): Future[util.Iterator[structure.Edge]] = {
    val s2EdgeIds = edgeIds.collect {
      case s2Edge: S2EdgeLike => s2Edge.id().asInstanceOf[EdgeId]
      case id: EdgeId => id
      case s: String => EdgeId.fromString(s)
    }
    val edgesToFetch = for {
      id <- s2EdgeIds
    } yield {
      elementBuilder.toEdge(id.srcVertexId, id.tgtVertexId, id.labelName, id.direction)
    }

    checkEdges(edgesToFetch).map { stepResult =>
      val ls = new util.ArrayList[structure.Edge]
      stepResult.edgeWithScores.foreach { es => ls.add(es.edge) }
      ls.iterator()
    }
  }

  def tx(): Transaction = {
    if (!features.graph.supportsTransactions) throw Graph.Exceptions.transactionsNotSupported
    ???
  }

  def variables(): Variables = new S2GraphVariables

  def configuration(): Configuration = apacheConfiguration

  def addVertex(label: String): Vertex = {
    if (label == null) throw Element.Exceptions.labelCanNotBeNull
    if (label.isEmpty) throw Element.Exceptions.labelCanNotBeEmpty

    addVertex(Seq(T.label, label): _*)
  }

  def makeVertex(idValue: AnyRef, kvsMap: Map[String, AnyRef]): S2VertexLike = {
    idValue match {
      case vId: VertexId =>
        elementBuilder.toVertex(vId.column.service.serviceName, vId.column.columnName, vId, kvsMap)
      case _ =>
        val serviceColumnNames = kvsMap.getOrElse(T.label.toString, DefaultColumnName).toString

        val names = serviceColumnNames.split(S2Vertex.VertexLabelDelimiter)
        val (serviceName, columnName) =
          if (names.length == 1) (DefaultServiceName, names(0))
          else throw new RuntimeException("malformed data on vertex label.")

        elementBuilder.toVertex(serviceName, columnName, idValue, kvsMap)
    }
  }

  def addVertex(kvs: AnyRef*): structure.Vertex = {
    if (!features().vertex().supportsUserSuppliedIds() && kvs.contains(T.id)) {
      throw Vertex.Exceptions.userSuppliedIdsNotSupported
    }

    val kvsMap = S2Property.kvsToProps(kvs)
    kvsMap.get(T.id.name()) match {
      case Some(idValue) if !S2Property.validType(idValue) =>
        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported()
      case _ =>
    }

    kvsMap.foreach { case (k, v) => S2Property.assertValidProp(k, v) }

    if (kvsMap.contains(T.label.name()) && kvsMap(T.label.name).toString.isEmpty)
      throw Element.Exceptions.labelCanNotBeEmpty

    val vertex = kvsMap.get(T.id.name()) match {
      case None => // do nothing
        val id = nextLocalLongId
        makeVertex(Long.box(id), kvsMap)
      case Some(idValue) if S2Property.validType(idValue) =>
        makeVertex(idValue, kvsMap)
      case _ =>
        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported
    }

    addVertexInner(vertex)

    vertex
  }

  def addVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2VertexLike = {
    val vertex = newVertex(id, ts, props, op, belongLabelIds)

    val future = mutateVertices(Seq(vertex), withWait = true).map { rets =>
      if (rets.forall(_.isSuccess)) vertex
      else throw new RuntimeException("addVertex failed.")
    }
    Await.ready(future, WaitTimeout)

    vertex
  }

  def addVertexInner(vertex: S2VertexLike): S2VertexLike = {
    val future = mutateVertices(Seq(vertex), withWait = true).flatMap { rets =>
      if (rets.forall(_.isSuccess)) {
        indexProvider.mutateVerticesAsync(Seq(vertex))
      } else throw new RuntimeException("addVertex failed.")
    }
    Await.ready(future, WaitTimeout)

    vertex
  }

  /* tp3 only */
  def addEdge(srcVertex: S2VertexLike, labelName: String, tgtVertex: Vertex, kvs: AnyRef*): Edge = {
    val containsId = kvs.contains(T.id)

    tgtVertex match {
      case otherV: S2VertexLike =>
        if (!features().edge().supportsUserSuppliedIds() && containsId) {
          throw Exceptions.userSuppliedIdsNotSupported()
        }

        val props = S2Property.kvsToProps(kvs)

        props.foreach { case (k, v) => S2Property.assertValidProp(k, v) }

        //TODO: direction, operation, _timestamp need to be reserved property key.

        try {
          val direction = props.get("direction").getOrElse("out").toString
          val ts = props.get(LabelMeta.timestamp.name).map(_.toString.toLong).getOrElse(System.currentTimeMillis())
          val operation = props.get("operation").map(_.toString).getOrElse("insert")
          val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))
          val dir = GraphUtil.toDir(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))
          val propsPlusTs = props ++ Map(LabelMeta.timestamp.name -> ts)
          val propsWithTs = label.propsToInnerValsWithTs(propsPlusTs, ts)
          val op = GraphUtil.toOp(operation).getOrElse(throw new RuntimeException(s"$operation is not supported."))

          val edge = newEdge(srcVertex, otherV, label, dir, op = op, version = ts, propsWithTs = propsWithTs)

          val future = mutateEdges(Seq(edge), withWait = true).flatMap { rets =>
            indexProvider.mutateEdgesAsync(Seq(edge))
          }
          Await.ready(future, WaitTimeout)

          edge
        } catch {
          case e: LabelNotExistException => throw new java.lang.IllegalArgumentException(e)
        }
      case null => throw new java.lang.IllegalArgumentException
      case _ => throw new RuntimeException("only S2Graph vertex can be used.")
    }
  }

  def close(): Unit = {
    shutdown()
  }

  def compute[C <: GraphComputer](aClass: Class[C]): C = ???

  def compute(): GraphComputer = {
    if (!features.graph.supportsComputer) {
      throw Graph.Exceptions.graphComputerNotSupported
    }
    ???
  }

  def io[I <: Io[_ <: GraphReader.ReaderBuilder[_ <: GraphReader], _ <: GraphWriter.WriterBuilder[_ <: GraphWriter], _ <: Mapper.Builder[_]]](builder: Io.Builder[I]): I = {
    builder.graph(this).registry(S2GraphIoRegistry.instance).create().asInstanceOf[I]
  }

  override def toString(): String = "[s2graph]"
}
