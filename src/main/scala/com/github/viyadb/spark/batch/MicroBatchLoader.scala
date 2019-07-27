package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.TableConf
import com.github.viyadb.spark.streaming.parser.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime

import scala.util.Try

/**
  * Utilities for loading real-time micro-batch data
  */
class MicroBatchLoader(tableConf: TableConf) extends Serializable {

  protected val microBatchSchema = new OutputSchema(tableConf)

  protected val columnIndices: Array[Int] = getInputColumnIndices

  /**
    * @return mapping between schema and column indices
    */
  protected def getInputColumnIndices: Array[Int] = {
    val inputCols = microBatchSchema.columns.zipWithIndex.toMap
    microBatchSchema.schema.fields.map(field => inputCols(field.name))
  }

  protected def parseTime(value: String, fieldIdx: Int): java.sql.Timestamp = {
    microBatchSchema.timeFormats(fieldIdx).map(format => format.parse(value)).getOrElse(
      new java.sql.Timestamp(
        Try {
          DateTime.parse(value).getMillis
        }.getOrElse(value.toLong)
      )
    )
  }

  /**
    * Parses record from string values according to schema
    *
    * @param values String field values
    * @return record
    */
  def parseInputRow(values: Seq[String]): Record = {
    new Record(
      microBatchSchema.indexedSchema.map { case (field, fieldIdx) =>
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

  def createDataFrame(rdd: RDD[Record]): DataFrame = {
    SparkSession.builder().getOrCreate().createDataFrame(rdd.asInstanceOf[RDD[Row]], microBatchSchema.schema)
  }
}
