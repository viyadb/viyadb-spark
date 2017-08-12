package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class TimeTruncator(config: JobConf) extends Processor(config) {

  protected def truncate(column: String, format: String)(df: DataFrame): DataFrame = {
    if (format == "second") {
      df
    } else {
      val newCol = format match {
        case "year" | "month" => trunc(df(column), format)
        case "day" => date_sub(df(column), 0)
        case "hour" => (floor(df(column).cast(LongType) / 3600) * 3600).cast(TimestampType)
        case "minute" => (floor(df(column).cast(LongType) / 60) * 60).cast(TimestampType)
      }
      df.withColumn(column, newCol)
    }
  }

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
