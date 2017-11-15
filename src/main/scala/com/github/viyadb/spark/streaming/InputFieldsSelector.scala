package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Selects dimensions and metrics from the input fields
  *
  * @param config Job configuration
  */
class InputFieldsSelector(config: JobConf) extends Processor(config) {

  override def process(df: DataFrame): DataFrame = {
    val selectCols = (config.table.dimensions ++ config.table.metrics.filter(_.`type` != "count")).map(field =>
      col(field.inputField()).as(field.name))

    df.select(selectCols: _ *)
  }
}
