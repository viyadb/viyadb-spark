package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs.{JobConf, ParseSpecConf}
import com.github.viyadb.spark.record.{Record, RecordFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class RecordFactory(config: JobConf) extends RecordFormat(config) {

  private val parseSpec = config.table.realTime.parseSpec.getOrElse(ParseSpecConf(""))

  private val javaValueParser = new JavaValueParser(
    parseSpec.nullNumericAsZero.getOrElse(true), parseSpec.nullStringAsEmpty.getOrElse(true))

  override def getInputColumns(): Seq[String] = {
    config.table.realTime.parseSpec.flatMap(_.columns).getOrElse(
      config.table.dimensions.map(_.name) ++ config.table.metrics.filter(_.`type` != "count").map(_.name)
    )
  }

  /**
    * @return mapping between schema field names and paths used to extract fields from an hierarchical structure
    *         (like Json, Avro, etc)
    */
  def getColumnMapping(): Option[Array[String]] = {
    parseSpec.fieldMapping
      .map(mapping => inputSchema.fields.map(f => mapping.get(f.name).get))
  }

  /**
    * Parses record from Java types values
    *
    * @param values Java types values
    * @return record
    */
  def parseJavaObjects(values: Array[_ <: Object]): Record = {
    new Record(indexedInputSchema.map { case (field, fieldIdx) =>
      val value = values(fieldIdx)
      field.dataType match {
        case IntegerType => javaValueParser.parseInt(value)
        case LongType => javaValueParser.parseLong(value)
        case DoubleType => javaValueParser.parseDouble(value)
        case TimestampType => javaValueParser.parseTimestamp(value).getOrElse(
          parseTime(javaValueParser.parseString(value), fieldIdx)
        )
        case StringType => javaValueParser.parseString(value)
      }
    })
  }

  /**
    * Create record from received string
    *
    * @param meta    Some metadata (topic in case of Kafka)
    * @param content Received content
    * @return generated record as Row or <code>None</code> in case it couldn't be generated for some reason
    */
  def createRecord(meta: String, content: String): Option[Row] = None

  /**
    * Create Spark data frame for RDD of records
    *
    * @param rdd RDD of records (rows)
    * @return Spark data frame
    */
  def createDataFrame(rdd: RDD[Row]): DataFrame = {
    SparkSession.builder().getOrCreate().createDataFrame(rdd, inputSchema)
  }
}

object RecordFactory extends Logging {
  def create(config: JobConf): RecordFactory = {
    val factory = config.table.realTime.recordFactoryClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[RecordFactory]
    ).getOrElse(
      config.table.realTime.parseSpec.map(parseSpec =>
        parseSpec.format match {
          case "tsv" => new TsvRecordFactory(config)
          case "json" => new JsonRecordFactory(config)
          case _ => throw new IllegalArgumentException("Unsupported record format specified in parse spec!")
        }
      ).getOrElse(
        new RecordFactory(config)
      )
    )
    logInfo(s"Created record factory: ${factory.getClass.getName}")
    factory
  }
}