package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame

/**
  * Selects and formats fields as they are written to a target file.
  *
  * @param config Job configuration
  */
class FieldSelector(config: JobConf) extends Processor(config) {

  private val selectCols = config.table.dimensions.map(_.name) ++ config.table.metrics.map(_.name)

  override def process(df: DataFrame): DataFrame = {
    df.select(selectCols.head, selectCols.tail: _*)
  }
}
