package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.schema._


case class LabelSchema(labelService: Map[Int, String],
                       labels: Map[Int, Label],
                       labelIndices: Map[Int, Map[Byte, LabelIndex]],
                       labelIndexLabelMetas: Map[Int, Map[Byte, Array[LabelMeta]]],
                       labelMetas: Map[Int, Map[Byte, LabelMeta]])