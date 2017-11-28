package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.batch.OutputSchema
import org.apache.spark.sql.DataFrame

/**
  * Selects and formats fields as they are written to a target file.
  */
class OutputFieldsSelector(tableConf: TableConf) extends Processor {

  private val selectCols = OutputSchema.columnNames(tableConf)

  override def process(df: DataFrame): DataFrame = {
    df.select(selectCols.head, selectCols.tail: _*)
  }
}
