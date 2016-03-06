package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.{IndexEdge, Vertex, TestCommonWithModels}
import com.kakao.s2graph.core.types._
import org.scalatest.{FunSuite, Matchers}


class IndexEdgeTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()

  /** note that props have to be properly set up for equals */
  test("test serializer/deserializer for index edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, labelV4.schemaVersion)
    val to = InnerVal.withLong(101, labelV4.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(labelV4.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, labelV4.schemaVersion)
    val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
      1.toByte -> InnerVal.withDouble(2.1, labelV4.schemaVersion))
    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdgeOpt = graph.storage.indexEdgeDeserializer(labelV4.schemaVersion).fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, labelV4.schemaVersion, None)

    _indexEdgeOpt should not be empty
    indexEdge should be(_indexEdgeOpt.get)

  }

  test("test serializer/deserializer for degree edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, labelV4.schemaVersion)
    val to = InnerVal.withStr("0", labelV4.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(labelV4.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, labelV4.schemaVersion)
    val props = Map(
      LabelMeta.degreeSeq -> InnerVal.withLong(10, labelV4.schemaVersion),
      LabelMeta.timeStampSeq -> tsInnerVal)

    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdgeOpt = graph.storage.indexEdgeDeserializer(labelV4.schemaVersion).fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, labelV4.schemaVersion, None)

    _indexEdgeOpt should not be empty
    indexEdge should be(_indexEdgeOpt.get)
  }

  test("test serializer/deserializer for incrementCount index edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, labelV4.schemaVersion)
    val to = InnerVal.withLong(101, labelV4.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(labelV4.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, labelV4.schemaVersion)
    val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
      1.toByte -> InnerVal.withDouble(2.1, labelV4.schemaVersion),
      LabelMeta.countSeq -> InnerVal.withLong(10, labelV4.schemaVersion))

    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdgeOpt = graph.storage.indexEdgeDeserializer(labelV4.schemaVersion).fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, labelV4.schemaVersion, None)
    _indexEdgeOpt should not be empty
    indexEdge should be(_indexEdgeOpt.get)
  }
}