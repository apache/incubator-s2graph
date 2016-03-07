/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//package com.kakao.s2graph.core.types
//
//import com.kakao.s2graph.core.types2._
//import com.kakao.s2graph.core.{TestCommonWithModels, TestCommon}
//import org.scalatest.{Matchers, FunSuite}
//
///**
// * Created by shon on 5/29/15.
// */
//class EdgeTypeTest extends FunSuite with Matchers with TestCommon {
//
//  import HBaseType.{VERSION1, VERSION2}
//  def vertexId(innerVal: InnerValLike) = VertexId(testColumnId, innerVal)
//  def sourceVertexId(innerVal: InnerValLike) = SourceVertexId(testColumnId, innerVal)
//  def targetVertexId(innerVal: InnerValLike) = TargetVertexId(testColumnId, innerVal)
//  val skipHashBytes = true
//
//  test("test edge row key order with int source vertex id version 1") {
//    val version = VERSION1
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeRowKey(sourceVertexId(innerVal), testLabelWithDir, testLabelOrderSeq, isInverted = false)(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeRowKey.fromBytes(bytes, 0, bytes.length, version)
//    testOrder(idxPropsLs, intInnerVals, skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//  test("test edge row key order with int source vertex id version 2") {
//    val version = VERSION2
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeRowKey(sourceVertexId(innerVal), testLabelWithDir, testLabelOrderSeq, isInverted = false)(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeRowKey.fromBytes(bytes, 0, bytes.length, version)
//    testOrder(idxPropsLsV2, intInnerValsV2, skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//
//  test("test edge row qualifier with int target vertex id version 1") {
//    val version = VERSION1
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeQualifier(idxProps, targetVertexId(innerVal), testOp)(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeQualifier.fromBytes(bytes, 0, bytes.length, version)
//
//    testOrder(idxPropsLs, intInnerVals, !skipHashBytes)(serializer, deserializer) shouldBe true
//    testOrderReverse(idxPropsLs, intInnerVals, !skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//  test("test edge row qualifier with int target vertex id version 2") {
//    val version = VERSION2
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeQualifier(idxProps, targetVertexId(innerVal), testOp)(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeQualifier.fromBytes(bytes, 0, bytes.length, version)
//
//    testOrder(idxPropsLsV2, intInnerValsV2, !skipHashBytes)(serializer, deserializer) shouldBe true
//    testOrderReverse(idxPropsLsV2, intInnerValsV2, !skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//
//  test("test edge row qualifier inverted with int target vertex id version 1") {
//    val version = VERSION1
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeQualifierInverted(targetVertexId(innerVal))(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeQualifierInverted.fromBytes(bytes, 0, bytes.length, version)
//
//    testOrder(idxPropsLs, intInnerVals, !skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//  test("test edge row qualifier inverted with int target vertex id version 2") {
//    val version = VERSION2
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      EdgeQualifierInverted(targetVertexId(innerVal))(version)
//    val deserializer = (bytes: Array[Byte]) => EdgeQualifierInverted.fromBytes(bytes, 0, bytes.length, version)
//
//    testOrder(idxPropsLsV2, intInnerValsV2, !skipHashBytes)(serializer, deserializer) shouldBe true
//  }
//
//}
