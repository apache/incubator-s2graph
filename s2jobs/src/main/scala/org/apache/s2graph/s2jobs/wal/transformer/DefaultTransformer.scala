package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.s2jobs.task.TaskConf

case class DefaultTransformer(taskConf: TaskConf) extends Transformer(taskConf)
