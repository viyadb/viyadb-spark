package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.{Aggregator, FieldSelector, ProcessorChain, TimeTruncator}

class StreamingProcessor(config: JobConf) extends ProcessorChain(config, Seq(
  new TimeTruncator(config),
  new Aggregator(config),
  new FieldSelector(config)
))
