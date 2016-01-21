package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData, Params}

import scala.reflect.ClassTag

abstract class Source[O <: Data : ClassTag](params: Params) extends BaseDataProcessor[EmptyData, O](params)
