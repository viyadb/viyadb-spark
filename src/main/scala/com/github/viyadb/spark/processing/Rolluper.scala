package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame

class Rolluper(config: JobConf) extends Processor(config) {

  override def process(df: DataFrame): DataFrame = {
    config.table.dimensions.filter(d => d.isTimeType())
    df
  }
}
