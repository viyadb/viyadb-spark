package com.github.viyadb.spark.processing

import com.github.viyadb.spark.TableConfig
import org.apache.spark.sql.DataFrame

class ProcessorChain(table: TableConfig.Table, chain: Seq[Processor]) extends Processor(table) {

  val composedProcessors = chain.map(p => (df: DataFrame) => p.process(df)).reduceLeft(_ compose _)

  override def process(df: DataFrame): DataFrame = {
    composedProcessors(df)
  }
}
