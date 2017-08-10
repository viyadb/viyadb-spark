package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TimeTruncator(config: JobConf) extends Processor(config) {

  protected def truncate(column: String, format: String)(df: DataFrame): DataFrame = {
    if (format == "second") {
      df
    } else {
      val newCol = format match {
        case "year" | "month" => trunc(df(column), format)
        case "day" => date_sub(df(column), 0)
        case "hour" => (floor(unix_timestamp(df(column)) / 3600) * 3600).cast("timestamp")
        case "minute" => (floor(unix_timestamp(df(column)) / 60) * 60).cast("timestamp")
      }
      df.withColumn(column, newCol)
    }
  }

  @transient
  private val truncateFuncs = config.table.dimensions.filter(d => d.isTimeType() && d.granularity.nonEmpty)
    .map(dim => truncate(dim.name, dim.granularity.get)(_))

  override def process(df: DataFrame): DataFrame = {
    if (truncateFuncs.isEmpty) {
      df
    } else {
      truncateFuncs.reduceLeft(_ compose _)(df)
    }
  }
}
