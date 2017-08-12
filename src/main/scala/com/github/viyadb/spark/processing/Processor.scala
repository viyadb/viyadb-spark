package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame

/**
  * Abstract class for processing data frame
  *
  * @param config Job configuration
  */
abstract class Processor(config: JobConf) extends Serializable {

  def process(df: DataFrame): DataFrame
}