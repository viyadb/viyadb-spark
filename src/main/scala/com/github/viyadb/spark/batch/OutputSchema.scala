package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.util.TimeUtil
import com.github.viyadb.spark.util.TimeUtil.TimeFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Utilities for writing pre-aggregated results
  */
object OutputSchema {

  val SYNTHETIC_COUNT_FIELD = "__viyadb_count"

  /**
    * @return Output column names
    */
  def columnNames(table: TableConf): Seq[String] = {
    val cols = (table.dimensions ++ table.metrics).map(_.name).toBuffer
    if (!table.hasCountMetric) {
      cols.append(SYNTHETIC_COUNT_FIELD)
    }
    return cols
  }

  /**
    * @return Schema corresponding to output columns
    */
  def schema(table: TableConf): StructType = {
    val fieldTypes = (table.dimensions ++ table.metrics).map(col => StructField(col.name, col.dataType)).toBuffer
    if (!table.hasCountMetric) {
      fieldTypes.append(StructField(SYNTHETIC_COUNT_FIELD, LongType))
    }
    StructType(fieldTypes)
  }
}

class OutputSchema(tableConf: TableConf) extends Serializable {

  protected val outputSchema = OutputSchema.schema(tableConf)

  protected val indexedOutputSchema = outputSchema.fields.zipWithIndex

  protected val timeFormats = getTimeFormats()

  /**
    * @return time formatters per field index
    */
  private def getTimeFormats(): Array[Option[TimeFormat]] = {
    outputSchema.fields.map { field =>
      tableConf.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType)
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
