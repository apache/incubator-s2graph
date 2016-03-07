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
//import com.kakao.s2graph.core.TestCommon
//import com.kakao.s2graph.core.types2._
//import org.apache.hadoop.hbase.util.Bytes
//import org.scalatest.{Matchers, FunSuite}
//
///**
// * Created by shon on 5/29/15.
// */
//class VertexTypeTest extends FunSuite with Matchers with TestCommon {
//
//
//  import HBaseType.{VERSION2, VERSION1}
//  val skipHashBytes = true
////
////  def functions = {
////    for {
////      version <- List(VERSION1, VERSION2)
////    } yield {
////      val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
////        VertexRowKey(VertexId(testColumnId, innerVal))(version)
////      val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, version)
////      (serializer, deserializer, version)
////    }
////  }
//
//  test("test vertex row key order with int id type version 1") {
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      VertexRowKey(VertexId(testColumnId, innerVal))(VERSION1)
//    val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, VERSION1)
//    testOrder(idxPropsLs, intInnerVals, skipHashBytes)(serializer, deserializer)
//  }
//  test("test vertex row key order with int id type version 2") {
//    val serializer = (idxProps: Seq[(Byte, InnerValLike)], innerVal: InnerValLike) =>
//      VertexRowKey(VertexId(testColumnId, innerVal))(VERSION2)
//    val deserializer = (bytes: Array[Byte]) => VertexRowKey.fromBytes(bytes, 0, bytes.length, VERSION2)
//    testOrder(idxPropsLsV2, intInnerValsV2, skipHashBytes)(serializer, deserializer)
//  }
//
//
//}
