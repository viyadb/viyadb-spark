package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.batch.OutputFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Utilities for rolling up data set according to table definition
  */
case class Aggregator(config: JobConf) extends Processor(config) {

  override def process(df: DataFrame): DataFrame = {
    val aggCols = (config.table.dimensions.map(_.name)
      ++ config.table.metrics.filter(_.`type` == "bitset").map(_.name))
      .distinct.map(col(_))

    // Add synthetic count that might be needed by some other functions:
    val countMetrics = config.table.metrics.filter(_.`type` == "count")
    val addCounts = if (countMetrics.nonEmpty) {
      countMetrics
        .map(metric => (df: DataFrame) => if (df.columns.contains(metric.name)) df else df.withColumn(metric.name, lit(1)))
        .reduceLeft(_ compose _)
    } else {
      (df: DataFrame) => df.withColumn(OutputFormat.SYNTHETIC_COUNT_FIELD, lit(1))
    }

    val aggExprs = config.table.metrics.filter(_.`type` != "bitset").map { metric =>
      (metric.`type` match {
        case "count" => sum(metric.name)
        case other => other.split("_")(1) match {
          case "sum" => sum(metric.name)
          case "min" => min(metric.name)
          case "max" => max(metric.name)
          case "avg" => sum(metric.name)
        }
      }).as(metric.name)
    }

    val aggWithCount = if (countMetrics.nonEmpty) aggExprs else
      aggExprs :+ sum(OutputFormat.SYNTHETIC_COUNT_FIELD).as(OutputFormat.SYNTHETIC_COUNT_FIELD)

    df.transform(addCounts).groupBy(aggCols: _*).agg(aggWithCount.head, aggWithCount.tail: _*)
  }
}