package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.processing.{Aggregator, OutputFieldsSelector, ProcessorChain}

class BatchProcessor(tableConf: TableConf) extends ProcessorChain(
  Aggregator(tableConf),
  new OutputFieldsSelector(tableConf)
)
