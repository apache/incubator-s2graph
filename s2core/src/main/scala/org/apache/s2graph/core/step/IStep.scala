package org.apache.s2graph.core.step

import org.apache.s2graph.core._
import rx.lang.scala.Observable

import scala.language.higherKinds
import scala.language.existentials

trait RxStep[-A, +B] extends (A => Observable[B])

object RxStep {

  case class VertexFetchStep(g: S2GraphLike) extends RxStep[Seq[S2VertexLike], S2VertexLike] {
    override def apply(vertices: Seq[S2VertexLike]): Observable[S2VertexLike] = {
      Observable.from(vertices)
    }
  }

  case class EdgeFetchStep(g: S2GraphLike, qp: QueryParam) extends RxStep[S2VertexLike, S2EdgeLike] {
    override def apply(v: S2VertexLike): Observable[S2EdgeLike] = {
      implicit val ec = g.ec

      val step = org.apache.s2graph.core.Step(Seq(qp))
      val q = Query(Seq(v), steps = Vector(step))

      val f = g.getEdges(q).map { stepResult =>
        val edges = stepResult.edgeWithScores.map(_.edge)
        Observable.from(edges)
      }

      Observable.from(f).flatten
    }
  }

  private def merge[A, B](steps: RxStep[A, B]*): RxStep[A, B] = new RxStep[A, B] {
    override def apply(in: A): Observable[B] =
      steps.map(_.apply(in)).toObservable.flatten
  }

  def toObservable(q: Query)(implicit graph: S2GraphLike): Observable[S2EdgeLike] = {
    val v1: Observable[S2VertexLike] = VertexFetchStep(graph).apply(q.vertices)

    val serialSteps = q.steps.map { step =>
      val parallelSteps = step.queryParams.map(qp => EdgeFetchStep(graph, qp))
      merge(parallelSteps: _*)
    }

    v1.flatMap { v =>
      val initOpt = serialSteps.headOption.map(_.apply(v))

      initOpt.map { init =>
        serialSteps.tail.foldLeft(init) { case (prev, next) =>
          prev.map(_.tgtForVertex).flatMap(next)
        }
      }.getOrElse(Observable.empty)
    }
  }
}
