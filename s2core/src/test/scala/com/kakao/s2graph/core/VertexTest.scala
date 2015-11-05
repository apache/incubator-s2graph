//package com.kakao.s2graph.core
//
//import com.kakao.s2graph.core.types.{VertexId, InnerVal, InnerValLike}
//import org.scalatest.{Matchers, FunSuite}
//
//
///**
// * Created by shon on 5/29/15.
// */
//class VertexTest extends FunSuite with Matchers with TestCommonWithModels with TestCommon {
//
//  import types.HBaseType.{VERSION1, VERSION2}
//  val idxPropsList = idxPropsLs.map { seq => seq.map { kv => kv._1.toInt -> kv._2 }}
//  val idxPropsListV2 = idxPropsLsV2.map { seq => seq.map { kv => kv._1.toInt -> kv._2 }}
//  def equalsExact(left: Vertex, right: Vertex) = {
//    left.id == right.id && left.ts == right.ts &&
//      left.props == right.props && left.op == right.op
//  }
//  def vertexId(innerVal: InnerValLike)(version: String) = {
//    val colId = if (version == VERSION2) columnV2.id.get else column.id.get
//    VertexId(colId, innerVal)
//  }
//
//  /** assumes innerVal is sorted */
//  def testVertexEncodeDecode(innerVals: Seq[InnerValLike],
//                             propsLs: Seq[Seq[(Int, InnerValLike)]], version: String) = {
//    for {
//      props <- propsLs
//    } {
//      val currentTs = BigDecimal(props.toMap.get(0.toByte).get.toString).toLong
//      val head = Vertex(vertexId(innerVals.head)(version), currentTs, props.toMap, op)
//      val start = head
//      var prev = head
//      for {
//        innerVal <- innerVals.tail
//      } {
//        var current = Vertex(vertexId(innerVal)(version), currentTs, props.toMap, op)
//        val puts = current.buildPutsAsync()
//        val kvs = for {put <- puts; kv <- putToKeyValues(put)} yield kv
//        val decodedOpt = Vertex(kvs, version)
//        val prevBytes = prev.rowKey.bytes.drop(GraphUtil.bytesForMurMurHash)
//        val currentBytes = current.rowKey.bytes.drop(GraphUtil.bytesForMurMurHash)
//        decodedOpt.isDefined shouldBe true
//        val isSame = equalsExact(decodedOpt.get, current)
//        val comp = lessThan(currentBytes, prevBytes)
//
//        println(s"current: $current")
//        println(s"decoded: ${decodedOpt.get}")
//        println(s"$isSame, $comp")
//        prev = current
//        isSame && comp shouldBe true
//      }
//    }
//  }
//
//  test("test with int innerVals as id version 1") {
//    testVertexEncodeDecode(intInnerVals, idxPropsList, VERSION1)
//  }
//  test("test with int innerVals as id version 2") {
//    testVertexEncodeDecode(intInnerValsV2, idxPropsListV2, VERSION2)
//  }
//  test("test with string stringVals as id versoin 2") {
//    testVertexEncodeDecode(stringInnerValsV2, idxPropsListV2, VERSION2)
//  }
//  //  test("test vertex encoding/decoding") {
//  //    val innerVal1 = new InnerVal(BigDecimal(10))
//  //    val innerVal2 = new InnerValV1(Some(10L), None, None)
//  //    println(s"${innerVal1.bytes.toList}")
//  //    println(s"${innerVal2.bytes.toList}")
//  //    val id1 = new CompositeId(0, innerVal1, isEdge = false, useHash = true)
//  //    val id2 = new CompositeIdV1(0, innerVal2, isEdge = false, useHash = true)
//  //    val ts = System.currentTimeMillis()
//  //    val v1 = Vertex(id1, ts)
//  //    val v2 = Vertex(id2, ts)
//  //
//  //    println(s"${v1.rowKey.bytes.toList}")
//  //    println(s"${v2.rowKey.bytes.toList}")
//  //  }
//}
