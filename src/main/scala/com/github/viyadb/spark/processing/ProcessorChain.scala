package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame

class ProcessorChain(config: JobConf, chain: Seq[Processor]) extends Processor(config) {

  val composedProcessors = chain.map(p => (df: DataFrame) => p.process(df)).reduceLeft(_ compose _)

  override def process(df: DataFrame): DataFrame = {
    composedProcessors(df)
  }
}
