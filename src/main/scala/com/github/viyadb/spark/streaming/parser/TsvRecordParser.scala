package com.github.viyadb.spark.streaming.parser

import com.github.viyadb.spark.Configs.{JobConf, ParseSpecConf}
import org.apache.spark.sql.types._

class TsvRecordParser(jobConf: JobConf) extends RecordParser(jobConf) {

  private val delimiter = parseSpec.delimiter.getOrElse("\t")

  private val columnIndices = inputSchema.map(f => parseSpec.columns.get.indexOf(f.name))

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

  override def parseRecord(topic: String, record: String) = {
    Some(
      parseInputRow(record.split(delimiter))
    )
  }
}
