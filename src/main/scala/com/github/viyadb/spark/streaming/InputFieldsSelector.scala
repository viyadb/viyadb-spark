package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.processing.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Selects dimensions and metrics from the input fields
  */
class InputFieldsSelector(tableConf: TableConf) extends Processor {

  override def process(df: DataFrame): DataFrame = {
    val selectCols = (tableConf.dimensions ++ tableConf.metrics.filter(!_.isCountType)).map(field =>
      col(field.inputField).as(field.name))

    df.select(selectCols: _ *)
  }
}
