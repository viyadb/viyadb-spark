package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.batch.OutputFormat
import org.apache.spark.sql.DataFrame

/**
  * Selects and formats fields as they are written to a target file.
  *
  * @param config Job configuration
  */
class OutputFieldsSelector(config: JobConf) extends Processor(config) {

  private val selectCols = OutputFormat.columnNames(config)

  override def process(df: DataFrame): DataFrame = {
    df.select(selectCols.head, selectCols.tail: _*)
  }
}
