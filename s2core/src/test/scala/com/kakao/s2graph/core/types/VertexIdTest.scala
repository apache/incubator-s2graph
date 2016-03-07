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
//import com.kakao.s2graph.core.types2.{HBaseType, VertexId, InnerValLike, InnerVal}
//import com.kakao.s2graph.core.{GraphUtil, TestCommonWithModels, TestCommon}
//import org.scalatest.{Matchers, FunSuite}
//
///**
// * Created by shon on 6/10/15.
// */
//class VertexIdTest extends FunSuite with Matchers with TestCommon with TestCommonWithModels {
//  import HBaseType.{VERSION1, VERSION2}
//
//  val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) => VertexId(testColumnId, innerVal)
//  def deserializer(version: String) = (bytes: Array[Byte]) => VertexId.fromBytes(bytes, 0, bytes.length, version)
//  val skipHashBytes = true
//  val emptyIndexPropsLs = Seq(Seq.empty[(Byte, InnerValLike)])
//  /** version 1 */
//  test("order of compositeId numeric v1") {
//    val version = VERSION1
//    testOrder(emptyIndexPropsLs, numInnerVals, skipHashBytes)(serializer, deserializer(version)) shouldBe true
//  }
//  /** string order in v1 is not actually descending. it depends on string length */
//  //  test("order of compositeId string v1") {
//  //    for {
//  //      isEdge <- List(true, false)
//  //      useHash <- List(true, false)
//  //    } {
//  //      testOrder(stringInnerVals, isEdge, useHash, VERSION1)
//  //    }
//  //  }
//  //  test("order of compositeId double v1") {
//  //    for {
//  //      isEdge <- List(true, false)
//  //      useHash <- List(true, false)
//  //    } {
//  //      testOrder(doubleInnerVals, isEdge, useHash, VERSION1)
//  //    }
//  //  }
//  /** version 2 */
//  test("order of compositeId numeric v2") {
//    val version = VERSION2
//    testOrder(emptyIndexPropsLs, numInnerValsV2, skipHashBytes)(serializer, deserializer(version)) shouldBe true
//  }
//  test("order of compositeId string v2") {
//    val version = VERSION2
//    testOrder(emptyIndexPropsLs, stringInnerValsV2, skipHashBytes)(serializer, deserializer(version)) shouldBe true
//  }
//  test("order of compositeId double v2") {
//    val version = VERSION2
//    testOrder(emptyIndexPropsLs, doubleInnerValsV2, skipHashBytes)(serializer, deserializer(version)) shouldBe true
//  }
//}
