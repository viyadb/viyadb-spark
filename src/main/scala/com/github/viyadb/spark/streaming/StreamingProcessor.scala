package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.processing.{Aggregator, OutputFieldsSelector, ProcessorChain, TimeTruncator}

class StreamingProcessor(tableConf: TableConf) extends ProcessorChain(
  new InputFieldsSelector(tableConf),
  new TimeTruncator(tableConf),
  new Aggregator(tableConf),
  new OutputFieldsSelector(tableConf)
)
