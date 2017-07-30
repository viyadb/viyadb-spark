package com.github.viyadb.spark.processing

import com.github.viyadb.spark.TableConfig
import org.apache.spark.sql.DataFrame

/**
  * Abstract class for processing data frame
  *
  * @param table Table configuration
  */
abstract class Processor(table: TableConfig.Table) extends Serializable {

  def process(df: DataFrame): DataFrame
}

object Processor {
  class DefaultProcessor(table: TableConfig.Table) extends ProcessorChain(table,
    Seq()
  )

  def create(table: TableConfig.Table): Processor = {
    if (table.processorClass.nonEmpty) {
      Class.forName(table.processorClass.get).getDeclaredConstructor(classOf[TableConfig.Table])
        .newInstance(table).asInstanceOf[Processor]
    } else {
      new DefaultProcessor(table)
    }
  }
}