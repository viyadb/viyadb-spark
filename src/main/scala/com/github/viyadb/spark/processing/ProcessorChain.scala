package com.github.viyadb.spark.processing

import org.apache.spark.sql.DataFrame


/**
  * This processor accepts other processors in order in which they will be applied, and creates
  * a composite processor. For example, if the input was: <code>Seq(a, b, c)</code>,
  * and <code>df</code> is a data frame, then what will be executed is the following:
  * <code>
  *   c.process(b.process(a.process(df)))
  * </code>
  *
  * @param chain Processors to apply on a data frame
  */
class ProcessorChain(chain: Processor*) extends Processor {

  private val composedProcessors = chain.map(p => (df: DataFrame) => p.process(df))
    .reverse.reduceLeft(_ compose _)

  override def process(df: DataFrame): DataFrame = {
    composedProcessors(df)
  }
}