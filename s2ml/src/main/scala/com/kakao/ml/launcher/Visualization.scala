package com.kakao.ml.launcher

import com.github.mdr.ascii._
import com.github.mdr.ascii.layout._

object LeftAlignedVertexRenderingStrategy extends VertexRenderingStrategy[Any] {

  def getPreferredSize(v: Any): Dimension = {
    val lines = splitLines(v.toString)
    Dimension(lines.size, lines.map(_.size).max)
  }

  def getText(v: Any, allocatedSize: Dimension): List[String] =
    splitLines(v.toString).take(allocatedSize.height).map { line ⇒
      line
    }

  private def splitLines(s: String): List[String] =
    s.split("(\r)?\n").toList match {
      case Nil | List("") ⇒ Nil
      case xs             ⇒ xs
    }

}


object Layouter {

  def renderGraph[T](graph: Graph[T]): String = {
    val (newGraph, reversedEdges) = new CycleRemover[T].removeCycles(graph)
    val layering = new LayeringCalculator[T].assignLayers(newGraph, reversedEdges.toSet)
    val layouter = new Layouter[Int](LeftAlignedVertexRenderingStrategy)
    val drawing = layouter.layout(LayerOrderingCalculator.reorder(layering))
    val cleanedUpDrawing = Compactifier.compactify(KinkRemover.removeKinks(drawing))
    Renderer.render(cleanedUpDrawing)
  }

}

trait Visualization {

  def getExecutionPlan(deps: List[(String, String)]): String = {
    val vertices = deps.flatMap(x => Iterable(x._1, x._2)).distinct
    val graph = Graph(vertices, deps)
    "\n" + Layouter.renderGraph(graph)
  }

}
