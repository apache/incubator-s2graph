package org.apache.s2graph.core.storage.rocks

import org.apache.s2graph.core._
import org.apache.s2graph.core.storage.{StorageIO, StorageSerDe, serde}
import org.apache.s2graph.core.types.HBaseType

class RocksStorageSerDe(val graph: S2GraphLike) extends StorageSerDe {

  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) =
    new serde.snapshotedge.tall.SnapshotEdgeSerializable(snapshotEdge)

  override def indexEdgeSerializer(indexEdge: IndexEdge) =
    new serde.indexedge.tall.IndexEdgeSerializable(indexEdge, RocksHelper.longToBytes)

  override def vertexSerializer(vertex: S2VertexLike) =
      new serde.vertex.tall.VertexSerializable(vertex, RocksHelper.intToBytes)


  private val snapshotEdgeDeserializer = new serde.snapshotedge.tall.SnapshotEdgeDeserializable(graph)
  override def snapshotEdgeDeserializer(schemaVer: String) = snapshotEdgeDeserializer

  private val indexEdgeDeserializable =
    new serde.indexedge.tall.IndexEdgeDeserializable(graph,
      RocksHelper.bytesToLong, tallSchemaVersions = HBaseType.ValidVersions.toSet)

  override def indexEdgeDeserializer(schemaVer: String) = indexEdgeDeserializable

  private val vertexDeserializer =
      new serde.vertex.tall.VertexDeserializable(graph)

  override def vertexDeserializer(schemaVer: String) = vertexDeserializer

}
