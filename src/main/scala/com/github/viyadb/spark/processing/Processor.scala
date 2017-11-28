package com.github.viyadb.spark.processing

import org.apache.spark.sql.DataFrame

/**
  * Abstract class for processing data frame per table
  */
abstract class Processor() extends Serializable {

  def process(df: DataFrame): DataFrame
}