package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame

/**
  * Abstract class for processing data frame
  *
  * @param config Table configuration
  */
abstract class Processor(config: JobConf) extends Serializable {

  def process(df: DataFrame): DataFrame
}

object Processor {

  def create(config: JobConf): Option[Processor] = {
    config.table.processorClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[Processor]
    )
  }
}