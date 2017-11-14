package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TsvRecordFactory(config: JobConf) extends RecordFactory(config) {

  private val delimiter = config.table.realTime.parseSpec.flatMap(_.delimiter).getOrElse("\t")

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
          case ByteType => value.toByte
          case ShortType => value.toShort
          case IntegerType => value.toInt
          case LongType => value.toLong
          case FloatType => value.toFloat
          case DoubleType => value.toDouble
          case TimestampType => parseTime(value, fieldIdx)
          case StringType => value
        }
      })
  }

  override def createRecord(meta: String, content: String): Option[Row] = {
    Some(
      parseInputRow(content.split(delimiter))
    )
  }
}