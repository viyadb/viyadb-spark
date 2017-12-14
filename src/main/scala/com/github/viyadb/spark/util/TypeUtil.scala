package com.github.viyadb.spark.util

import com.github.viyadb.spark.Configs.{DimensionConf, MetricConf}
import org.apache.spark.sql.types._

object TypeUtil {

  private def maxValueType(max: Option[Long]): DataType = {
    max.getOrElse((Integer.MAX_VALUE - 1).toLong) match {
      case x if x < Int.MaxValue => IntegerType
      case _ => LongType
    }
  }

  /**
    * Returns appropriate Spark data type for the given dimension
    */
  def dataType(dim: DimensionConf): DataType = {
    dim.`type`.getOrElse("string") match {
      case "string" => StringType
      case "numeric" => maxValueType(dim.max)
      case "time" | "microtime" => TimestampType
      case "byte" => ByteType
      case "ubyte" | "short" => ShortType
      case "ushort" | "int" => IntegerType
      case "uint" | "long" | "ulong" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case other => throw new IllegalArgumentException(s"Unknown dimension type: $other")
    }
  }

  /**
    * Returns appropriate Spark data type for the given metric
    */
  def dataType(metric: MetricConf): DataType = {
    metric.`type` match {
      case "count" => LongType
      case "bitset" => maxValueType(metric.max)
      case other => other.split("_")(0) match {
        case "byte" => ByteType
        case "ubyte" | "short" => ShortType
        case "ushort" | "int" => IntegerType
        case "uint" | "long" | "ulong" => LongType
        case "float" => FloatType
        case "double" => DoubleType
        case _ => throw new IllegalArgumentException(s"Unknown metric type: $other")
      }
    }
  }
}
