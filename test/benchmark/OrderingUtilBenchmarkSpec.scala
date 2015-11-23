package benchmark

import com.kakao.s2graph.core.OrderingUtil._
import com.kakao.s2graph.core.SeqMultiOrdering
import play.api.libs.json.{JsNumber, JsValue}
import play.api.test.PlaySpecification
import play.api.{Application => PlayApplication}

import scala.util.Random

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 9..
  */
class OrderingUtilBenchmarkSpec extends BenchmarkCommon with PlaySpecification {
  "OrderingUtilBenchmarkSpec" should {

    "performance MultiOrdering any" >> {
      val tupLs = (0 until 10) map { i =>
        Random.nextDouble() -> Random.nextLong()
      }

      val seqLs = tupLs.map { tup =>
        Seq(tup._1, tup._2)
      }

      val sorted1 = duration("TupleOrdering double,long") {
        (0 until 10000) foreach { _ =>
          tupLs.sortBy { case (x, y) =>
            -x -> -y
          }
        }
        tupLs.sortBy { case (x, y) =>
          -x -> -y
        }
      }.map { x => x._1 }

      val sorted2 = duration("MultiOrdering double,long") {
        (0 until 10000) foreach { _ =>
          seqLs.sorted(new SeqMultiOrdering[Any](Seq(false, false)))
        }
        seqLs.sorted(new SeqMultiOrdering[Any](Seq(false, false)))
      }.map { x => x.head }

      sorted1.toString() must_== sorted2.toString()
    }

    "performance MultiOrdering double" >> {
      val tupLs = (0 until 500) map { i =>
        Random.nextDouble() -> Random.nextDouble()
      }

      val seqLs = tupLs.map { tup =>
        Seq(tup._1, tup._2)
      }

      duration("MultiOrdering double") {
        (0 until 10000) foreach { _ =>
          seqLs.sorted(new SeqMultiOrdering[Double](Seq(false, false)))
        }
      }

      duration("TupleOrdering double") {
        (0 until 10000) foreach { _ =>
          tupLs.sortBy { case (x, y) =>
            -x -> -y
          }
        }
      }

      1 must_== 1
    }

    "performance MultiOrdering jsvalue" >> {
      val tupLs = (0 until 500) map { i =>
        Random.nextDouble() -> Random.nextLong()
      }

      val seqLs = tupLs.map { tup =>
        Seq(JsNumber(tup._1), JsNumber(tup._2))
      }

      val sorted1 = duration("TupleOrdering double,long") {
        (0 until 10000) foreach { _ =>
          tupLs.sortBy { case (x, y) =>
            -x -> -y
          }
        }
        tupLs.sortBy { case (x, y) =>
          -x -> -y
        }
      }

      val sorted2 = duration("MultiOrdering jsvalue") {
        (0 until 10000) foreach { _ =>
          seqLs.sorted(new SeqMultiOrdering[JsValue](Seq(false, false)))
        }
        seqLs.sorted(new SeqMultiOrdering[JsValue](Seq(false, false)))
      }

      1 must_== 1
    }
  }
}
