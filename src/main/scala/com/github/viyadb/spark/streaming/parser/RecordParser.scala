package com.github.viyadb.spark.streaming.parser

import com.github.viyadb.spark.Configs.{JobConf, ParseSpecConf}
import com.github.viyadb.spark.util.TimeUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class RecordParser(jobConf: JobConf) extends Serializable {

  protected val parseSpec = jobConf.indexer.realTime.parseSpec.getOrElse(ParseSpecConf())

  protected val inputSchema = mergedTablesSchema()

  protected val indexedInputSchema = inputSchema.fields.zipWithIndex

  private val timeFormats = inputSchema.map { fieldType =>
    parseSpec.timeFormats.flatMap(_.get(fieldType.name)).map(TimeUtil.strptime2JavaFormat(_))
  }.toArray

  /**
    * Returns schema containing all the columns from all tables.
    * The order of schema fields is not defined.
    */
  protected def mergedTablesSchema(): StructType = {
    val mergedFields = jobConf.tableConfigs.flatMap { tableConf =>
      (tableConf.dimensions ++ tableConf.metrics.filter(!_.isCountType))
    }.map(col => StructField(col.inputField, col.dataType)).distinct

    val fieldNames = mergedFields.map(_.name).toList
    if (fieldNames.distinct.size != fieldNames.size) {
      throw new IllegalArgumentException("Conflicting types are defined in following fields: " +
        fieldNames.diff(fieldNames.distinct).distinct.mkString(", "))
    }

    StructType(mergedFields)
  }

  /**
    * Parses input time field according to it's index in schema
    */
  protected def parseTime(value: String, fieldIdx: Int): java.sql.Timestamp = {
    timeFormats(fieldIdx).map(format => format.parse(value)).getOrElse(
      new java.sql.Timestamp(value.toLong)
    )
  }

  def createDataFrame(rdd: RDD[Record]): DataFrame = {
    SparkSession.builder().getOrCreate().createDataFrame(rdd.asInstanceOf[RDD[Row]], inputSchema)
  }

  /**
    * Parses record received as string chunk
    *
    * @param topic  Topic the record came from
    * @param record String representation of a record
    */
  def parseRecord(topic: String, record: String): Option[Record]
}

object RecordParser extends Logging {
  /**
    * Creates record parser according to the parse specification
    */
  def create(jobConf: JobConf): RecordParser = {
    val parseSpec = jobConf.indexer.realTime.parseSpec.getOrElse(ParseSpecConf())
    val factory = parseSpec.recordParserClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(jobConf).asInstanceOf[RecordParser]
    ).getOrElse(
      parseSpec.format match {
        case "tsv" => new TsvRecordParser(jobConf)
        case "json" => new JsonRecordParser(jobConf)
        case _ => throw new IllegalArgumentException("Parse specification doesn't define record parser type")
      }
    )
    logInfo(s"Created record factory: ${factory.getClass.getName}")
    factory
  }
}