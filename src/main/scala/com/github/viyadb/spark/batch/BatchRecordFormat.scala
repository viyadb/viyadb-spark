package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.record.RecordFormat
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class BatchRecordFormat(config: JobConf) extends RecordFormat(config) {

  override protected def getInputColumns() = {
    config.table.dimensions.map(_.name) ++ config.table.metrics.map(_.name)
  }

  def loadDataFrame(spark: SparkSession, path: String): DataFrame = {
    val rdd = spark.sparkContext.textFile(path)
      .map(c => parseInputRow(c.split("\t")).asInstanceOf[Row])

    spark.createDataFrame(rdd, schema)
  }
}
