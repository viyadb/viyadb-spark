package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.util.TimeUtil
import com.github.viyadb.spark.util.TimeUtil.TimeFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object OutputSchema {
  val SYNTHETIC_COUNT_FIELD = "__viyadb_count"
}

class OutputSchema(tableConf: TableConf) extends Serializable {

  val schema: StructType = getSchema

  val indexedSchema: Array[(StructField, Int)] = schema.fields.zipWithIndex

  val columns: Seq[String] = schema.map(_.name)

  val timeFormats: Array[Option[TimeFormat]] = getTimeFormats

  /**
    * @return time formatters per field index
    */
  private def getTimeFormats: Array[Option[TimeFormat]] = {
    schema.fields.map { field =>
      tableConf.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType)
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  /**
    * @return Schema corresponding to output columns
    */
  private def getSchema: StructType = {
    val fieldTypes = (tableConf.dimensions ++ tableConf.metrics)
      .map(col => StructField(col.name, col.dataType)).toBuffer
    if (!tableConf.hasCountMetric) {
      fieldTypes.append(StructField(OutputSchema.SYNTHETIC_COUNT_FIELD, LongType))
    }
    StructType(fieldTypes)
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
    indexedSchema.map { case (field, fieldIdx) =>
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
