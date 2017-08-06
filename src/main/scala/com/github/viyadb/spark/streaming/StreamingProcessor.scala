package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.{Aggregator, ProcessorChain, Rolluper}

class StreamingProcessor(config: JobConf) extends ProcessorChain(config, Seq(
  new Rolluper(config),
  new Aggregator(config)
))
