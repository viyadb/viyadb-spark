package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{DimensionConf, JobConf, MetricConf}
import com.github.viyadb.spark.util.TimeUtil
import com.github.viyadb.spark.util.TimeUtil.TimeFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Utilities for writing pre-aggregated results
  */
object OutputFormat {

  val SYNTHETIC_COUNT_FIELD = "__viyadb_count"

  def maxValueType(max: Option[Long]): DataType = {
    max.getOrElse((Integer.MAX_VALUE - 1).toLong) match {
      case x if x < Int.MaxValue => IntegerType
      case _ => LongType
    }
  }

  def dimensionDataType(dim: DimensionConf): DataType = {
    dim.`type`.getOrElse("string") match {
      case "string" => StringType
      case "numeric" => maxValueType(dim.max)
      case "time" | "microtime" => TimestampType
      case _ => throw new IllegalArgumentException(s"Unknown dimension type: ${dim.`type`}")
    }
  }

  def metricDataType(metric: MetricConf): DataType = {
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
      }
    }
  }

  /**
    * @return Output column names
    */
  def columnNames(config: JobConf): Seq[String] = {
    val cols = (config.table.dimensions.map(_.name) ++ config.table.metrics.map(_.name)).toBuffer
    if (!config.table.hasCountMetric()) {
      cols.append(SYNTHETIC_COUNT_FIELD)
    }
    return cols
  }

  /**
    * @return Schema corresponding to output columns
    */
  def schema(config: JobConf): StructType = {
    val fieldTypes = (config.table.dimensions.map(dim => StructField(dim.name, dimensionDataType(dim))) ++
      config.table.metrics.map(metric => StructField(metric.name, metricDataType(metric)))).toBuffer
    if (!config.table.hasCountMetric()) {
      fieldTypes.append(StructField(SYNTHETIC_COUNT_FIELD, LongType))
    }
    StructType(fieldTypes)
  }
}

class OutputFormat(config: JobConf) extends Serializable {

  protected val outputSchema = OutputFormat.schema(config)

  protected val indexedOutputSchema = outputSchema.fields.zipWithIndex

  protected val timeFormats = getTimeFormats()

  /**
    * @return time formatters per field index
    */
  private def getTimeFormats(): Array[Option[TimeFormat]] = {
    outputSchema.fields.map { field =>
      config.table.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType())
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  /**
    * Format date time according to the specified format
    *
    * @param time     Timestamp object
    * @param fieldIdx Schema field index
    * @return
    */
  protected def formatTime(time: java.util.Date, fieldIdx: Int): String = {
    timeFormats(fieldIdx).map(format => format.format(
      time match {
        case t: java.sql.Timestamp => t
        case d => new java.sql.Timestamp(d.getTime)
      }
    )).getOrElse(
      time.getTime.toString
    )
  }

  /**
    * Converts data frame row to TSV line
    *
    * @param row       Spark's data frame row
    * @param dropRight Omit last N columns from the result
    * @return
    */
  def toTsvLine(row: Row, dropRight: Int = 0): String = {
    val lastIdx = row.size - dropRight
    indexedOutputSchema.map { case (field, fieldIdx) =>
      if (fieldIdx < lastIdx) {
        val value = row(fieldIdx)
        field.dataType match {
          case StringType => value.asInstanceOf[String].replaceAll("[\t\n\r\u0000\\\\]", "").trim
          case TimestampType => formatTime(value.asInstanceOf[java.util.Date], fieldIdx)
          case _ => value
        }
      } else {
        None
      }
    }.filter(_ != None).mkString("\t")
  }
}
