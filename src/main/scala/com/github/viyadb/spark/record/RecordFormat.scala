package com.github.viyadb.spark.record

import java.text.SimpleDateFormat

import com.github.viyadb.spark.Configs.{DimensionConf, JobConf, MetricConf}
import com.github.viyadb.spark.util.TimeUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Base class containing utilities for parsing and formatting records
  *
  * @param config Job configuration
  */
abstract class RecordFormat(config: JobConf) extends Serializable with Logging {

  protected val inputSchema = getInputSchema()

  protected val indexedInputSchema = inputSchema.fields.zipWithIndex

  protected val outputSchema = getOutputSchema()

  protected val indexedOutputSchema = outputSchema.fields.zipWithIndex

  protected val timeFormats = getTimeFormats()

  protected val columnIndices = getInputColumnIndices()

  private def maxValueType(max: Option[Long]): DataType = {
    max.getOrElse((Integer.MAX_VALUE - 1).toLong) match {
      case x if x < Int.MaxValue => IntegerType
      case _ => LongType
    }
  }

  private def dimensionDataType(dim: DimensionConf): DataType = {
    dim.`type`.getOrElse("string") match {
      case "string" => StringType
      case "numeric" => maxValueType(dim.max)
      case "time" | "microtime" => TimestampType
      case _ => throw new IllegalArgumentException(s"Unknown dimension type: ${dim.`type`}")
    }
  }

  private def metricDataType(metric: MetricConf): DataType = {
    metric.`type` match {
      case "count" => LongType
      case "bitset" => maxValueType(metric.max)
      case other => other.split("_")(0) match {
        case "int" | "uint" => IntegerType
        case "long" | "ulong" => LongType
        case "double" => DoubleType
      }
    }
  }

  /**
    * @return Input column names
    */
  protected def getInputColumns(): Seq[String]

  /**
    * @return mapping between schema and column indices
    */
  private def getInputColumnIndices(): Array[Int] = {
    val inputCols = getInputColumns().zipWithIndex.toMap
    inputSchema.fields.map(field => inputCols.get(field.name).get)
  }

  /**
    * @return Schema corresponding to input columns
    */
  private def getInputSchema(): StructType = {
    val column2Type = (
      config.table.dimensions.map(dim => (dim.name, StructField(dim.name, dimensionDataType(dim)))) ++
        config.table.metrics.map(metric => (metric.name, StructField(metric.name, metricDataType(metric))))
      ).toMap

    StructType(getInputColumns().map(col => column2Type.get(col)).filter(_.nonEmpty).map(_.get))
  }

  /**
    * @return Schema corresponding to input columns
    */
  private def getOutputSchema(): StructType = {
    StructType(
      config.table.dimensions.map(dim => StructField(dim.name, dimensionDataType(dim))) ++
        config.table.metrics.map(metric => StructField(metric.name, metricDataType(metric)))
    )
  }

  /**
    * @return time formatters per field index
    */
  private def getTimeFormats(): Array[Option[SimpleDateFormat]] = {
    inputSchema.fields.map { field =>
      config.table.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType())
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  protected def parseTime(value: String, fieldIdx: Int): java.sql.Timestamp = {
    timeFormats(fieldIdx).map(format => new java.sql.Timestamp(format.parse(value).getTime)).getOrElse(
      new java.sql.Timestamp(value.toLong)
    )
  }

  /**
    * Parses record from string values according to schema
    *
    * @param values String field values
    * @return record
    */
  def parseInputRow(values: Array[String]): Record = {
    new Record(
      indexedInputSchema.map { case (field, fieldIdx) =>
        val value = values(columnIndices(fieldIdx))
        field.dataType match {
          case IntegerType => value.toInt
          case LongType => value.toLong
          case DoubleType => value.toDouble
          case TimestampType => parseTime(value, fieldIdx)
          case StringType => value
        }
      })
  }

  /**
    * Format date time according to the specified format
    *
    * @param time     Date time object
    * @param fieldIdx Schema field index
    * @return
    */
  protected def formatTime(time: java.util.Date, fieldIdx: Int): String = {
    timeFormats(fieldIdx).map(format => format.format(time)).getOrElse(
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
