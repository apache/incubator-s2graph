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

package org.apache.s2graph.core.storage

import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{GraphUtil, S2GraphLike, S2VertexLike, VertexMutator}

import scala.concurrent.{ExecutionContext, Future}

class DefaultOptimisticVertexMutator(graph: S2GraphLike,
                                     serDe: StorageSerDe,
                                     optimisticEdgeFetcher: OptimisticEdgeFetcher,
                                     optimisticMutator: OptimisticMutator,
                                     io: StorageIO) extends VertexMutator {

  def mutateVertex(zkQuorum: String, vertex: S2VertexLike, withWait: Boolean)(implicit ec: ExecutionContext): Future[MutateResponse] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      optimisticMutator.writeToStorage(zkQuorum,
        serDe.vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete)), withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(MutateResponse.Success) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      optimisticMutator.writeToStorage(zkQuorum, io.buildPutsAll(vertex), withWait)
    }
  }
}
