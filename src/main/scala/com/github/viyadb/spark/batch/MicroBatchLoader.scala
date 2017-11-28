package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.streaming.parser.Record
import com.github.viyadb.spark.util.TimeUtil
import com.github.viyadb.spark.util.TimeUtil.TimeFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Utilities for loading real-time micro-batch data
  */
class MicroBatchLoader(tableConf: TableConf) extends Serializable {

  protected val microBatchSchema = OutputSchema.schema(tableConf)

  protected val indexedMicroBatchSchema = microBatchSchema.fields.zipWithIndex

  protected val timeFormats = getTimeFormats()

  protected val columnIndices = getInputColumnIndices()

  /**
    * @return mapping between schema and column indices
    */
  protected def getInputColumnIndices(): Array[Int] = {
    val inputCols = OutputSchema.columnNames(tableConf).zipWithIndex.toMap
    microBatchSchema.fields.map(field => inputCols.get(field.name).get)
  }

  /**
    * @return time formatters per field index
    */
  protected def getTimeFormats(): Array[Option[TimeFormat]] = {
    microBatchSchema.fields.map { field =>
      tableConf.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType)
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  protected def parseTime(value: String, fieldIdx: Int): java.sql.Timestamp = {
    timeFormats(fieldIdx).map(format => format.parse(value)).getOrElse(
      new java.sql.Timestamp(value.toLong)
    )
  }

  /**
    * Parses record from string values according to schema
    *
    * @param values String field values
    * @return record
    */
  def parseInputRow(values: Array[String]): Row = {
    new Record(
      indexedMicroBatchSchema.map { case (field, fieldIdx) =>
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

  /**
    * Loads micro-batch data from given path as data frame
    *
    * @param path Path containing real-time data for a batch
    * @return data frame
    */
  def loadDataFrame(path: String): DataFrame = {
    val rdd = SparkSession.builder().getOrCreate().sparkContext.textFile(path)
      .mapPartitions(partition =>
        partition.map(content => parseInputRow(content.split("\t"))))
    createDataFrame(rdd)
  }

  def createDataFrame(rdd: RDD[Row]) = {
    SparkSession.builder().getOrCreate().createDataFrame(rdd, microBatchSchema)
  }
}
