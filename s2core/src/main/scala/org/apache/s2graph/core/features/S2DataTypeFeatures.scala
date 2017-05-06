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

package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

case class S2DataTypeFeatures() extends Features.DataTypeFeatures {

  // primitive types
  override def supportsBooleanValues(): Boolean = true

  override def supportsByteValues(): Boolean = false

  override def supportsDoubleValues(): Boolean = true

  override def supportsFloatValues(): Boolean = true

  override def supportsIntegerValues(): Boolean = true

  override def supportsLongValues(): Boolean = true

  // non-primitive types
  override def supportsMapValues(): Boolean = false

  override def supportsMixedListValues(): Boolean = false

  override def supportsBooleanArrayValues(): Boolean = false

  override def supportsByteArrayValues(): Boolean = false

  override def supportsDoubleArrayValues(): Boolean = false

  override def supportsFloatArrayValues(): Boolean = false

  override def supportsIntegerArrayValues(): Boolean = false

  override def supportsStringArrayValues(): Boolean = false

  override def supportsLongArrayValues(): Boolean = false

  override def supportsSerializableValues(): Boolean = false

  override def supportsStringValues(): Boolean = true

  override def supportsUniformListValues(): Boolean = false

}
