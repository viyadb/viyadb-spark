package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.{Aggregator, OutputFieldsSelector, ProcessorChain, TimeTruncator}

class StreamingProcessor(config: JobConf) extends ProcessorChain(config, Seq(
  new InputFieldsSelector(config),
  new TimeTruncator(config),
  new Aggregator(config),
  new OutputFieldsSelector(config)
))
