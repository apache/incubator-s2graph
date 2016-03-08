package org.apache.s2graph.counter.loader.models

import org.apache.s2graph.counter.models.CounterModel
import org.apache.s2graph.spark.config.S2ConfigFactory

case object DefaultCounterModel extends CounterModel(S2ConfigFactory.config)
