package com.github.viyadb.spark.processing

import com.github.viyadb.spark.TableConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, _}

/**
  * Utilities for rolling up data set according to table definition
  */
case class TableAggregator(table: TableConfig.Table) extends Processor(table) {

  override def process(df: DataFrame): DataFrame = {

    val aggCols = (table.batch.timeColumn.map(_.name).toList
      ++ table.dimensions.map(_.name)
      ++ table.metrics.filter(_.`type` == "bitset").map(_.name))
      .distinct.map(col(_))

    val aggExprs = table.metrics.filter(_.`type` != "bitset").map { metric =>
      (metric.`type` match {
        case "count" => sum(metric.name)
        case other => other.split("_")(1) match {
          case "sum" => sum(metric.name)
          case "min" => min(metric.name)
          case "max" => max(metric.name)
        }
      }).as(metric.name)
    }

    df.groupBy(aggCols: _*).agg(aggExprs.head, aggExprs.tail: _*)
  }
}