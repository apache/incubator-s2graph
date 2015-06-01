package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.TestCommon
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/29/15.
 */
class CompositeIdTest extends FunSuite with Matchers with TestCommon {
  /** these constants need to be sorted asc order for test to run */
  val strings = List(
    "abc", "abd", "ac", "aca", "b"
  ).map(InnerVal(_))

  val nums = {
    val decimals = (Long.MinValue until Long.MinValue + 10).map(BigDecimal(_)) ++
      (Int.MinValue until Int.MinValue + 10).map(BigDecimal(_)) ++
      (Int.MaxValue - 10 until Int.MaxValue).map(BigDecimal(_)) ++
      (Long.MaxValue - 10 until Long.MaxValue).map(BigDecimal(_))
    decimals.map(InnerVal(_))
  }
  val doubleNums = {
    val ls = (Double.MinValue until Double.MinValue + 2.0 by 0.2).map(BigDecimal(_)) ++
      (-9994.9 until -9999.1 by 1.1).map(BigDecimal(_)) ++
      (-128.0 until 128.0 by 1.2).map(BigDecimal(_)) ++
      (129.0 until 142.0 by 1.1).map(BigDecimal(_)) ++
      (Double.MaxValue - 10.0 until Double.MaxValue by 0.2).map(BigDecimal(_))
    ls.map(InnerVal(_))
  }

  val compositeIdFuncs = for {
    isEdge <- List(true, false)
    useHash <- List(false)
  } yield {
      val compositeIdCreateFunc = (idxProps: Seq[(Byte, InnerVal)], innerVal: InnerVal) =>
        CompositeId(testColumnId, innerVal, isEdge, useHash)
      val compositeIdFromBytesFunc = (bytes: Array[Byte]) => CompositeId(bytes, 0, isEdge, useHash)
      (compositeIdCreateFunc, compositeIdFromBytesFunc)
    }


  test("order of compositeId numeric") {
    val rets = compositeIdFuncs.map { case (createFunc, fromBytesFunc) =>
      testOrder(idxPropsLs, nums)(createFunc, fromBytesFunc) &&
        testOrder(idxPropsLs, doubleNums)(createFunc, fromBytesFunc)
    }
    println(rets)
    rets.forall(x => x) shouldBe true
  }
  test("order of compositeId strings") {
    val rets = compositeIdFuncs.map { case (createFunc, fromBytesFunc) =>
      testOrder(idxPropsLs, strings)(createFunc, fromBytesFunc)
    }
    println(rets)
    rets.forall(x => x) shouldBe true
  }
}
