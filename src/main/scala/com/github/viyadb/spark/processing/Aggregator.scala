package com.github.viyadb.spark.processing

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.batch.OutputSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Utilities for rolling up data set according to table definition
  */
case class Aggregator(tableConf: TableConf) extends Processor {

  override def process(df: DataFrame): DataFrame = {
    val aggCols = (tableConf.dimensions.map(_.name) ++ tableConf.metrics.filter(_.isBitsetType).map(_.name))
      .distinct.map(col(_))

    // Add synthetic count that might be needed by some other functions:
    val countMetrics = tableConf.metrics.filter(_.isCountType)
    val addCounts = if (countMetrics.nonEmpty) {
      countMetrics
        .map(metric => (df: DataFrame) => if (df.columns.contains(metric.name)) df else df.withColumn(metric.name, lit(1)))
        .reduceLeft(_ compose _)
    } else {
      (df: DataFrame) => df.withColumn(OutputSchema.SYNTHETIC_COUNT_FIELD, lit(1))
    }

    val aggExprs = tableConf.metrics.filter(!_.isBitsetType).map { metric =>
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
      aggExprs :+ sum(OutputSchema.SYNTHETIC_COUNT_FIELD).as(OutputSchema.SYNTHETIC_COUNT_FIELD)

    df.transform(addCounts).groupBy(aggCols: _*).agg(aggWithCount.head, aggWithCount.tail: _*)
  }
}