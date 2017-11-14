package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs.{JobConf, ParseSpecConf}
import com.github.viyadb.spark.batch.OutputFormat
import com.github.viyadb.spark.util.TimeUtil
import com.github.viyadb.spark.util.TimeUtil.TimeFormat
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class RecordFactory(config: JobConf) extends Serializable with Logging {

  protected val parseSpec = config.table.realTime.parseSpec.getOrElse(ParseSpecConf(""))

  protected val inputSchema = getInputSchema()

  protected val indexedInputSchema = inputSchema.fields.zipWithIndex

  protected val timeFormats = getTimeFormats()

  protected val columnIndices = getInputColumnIndices()

  /**
    * @return mapping between schema and column indices
    */
  protected def getInputColumnIndices(): Array[Int] = {
    val inputCols = getInputColumns().zipWithIndex.toMap
    inputSchema.fields.map(field => inputCols.get(field.name).get)
  }

  /**
    * @return time formatters per field index
    */
  protected def getTimeFormats(): Array[Option[TimeFormat]] = {
    inputSchema.fields.map { field =>
      config.table.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType())
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  protected def getInputColumns(): Seq[String] = {
    config.table.realTime.parseSpec.flatMap(_.columns).getOrElse(
      config.table.dimensions.map(_.name) ++ config.table.metrics.filter(_.`type` != "count").map(_.name)
    )
  }

  /**
    * @return Schema corresponding to input columns
    */
  protected def getInputSchema(): StructType = {
    val column2Type = (
      config.table.dimensions.map(dim => (dim.name, StructField(dim.name, OutputFormat.dimensionDataType(dim)))) ++
        config.table.metrics.map(metric => (metric.inputField(), StructField(metric.inputField(), OutputFormat.metricDataType(metric))))
      ).toMap

    StructType(getInputColumns().map(col => column2Type.get(col)).filter(_.nonEmpty).map(_.get))
  }

  /**
    * @return mapping between schema field names and paths used to extract fields from an hierarchical structure
    *         (like Json, Avro, etc)
    */
  protected def getColumnMapping(): Option[Array[String]] = {
    parseSpec.fieldMapping
      .map(mapping => inputSchema.fields.map(f => mapping.get(f.name).get))
  }

  protected def parseTime(value: String, fieldIdx: Int): java.sql.Timestamp = {
    timeFormats(fieldIdx).map(format => format.parse(value)).getOrElse(
      new java.sql.Timestamp(value.toLong)
    )
  }

  /**
    * Create record from received string
    *
    * @param meta    Some metadata (topic in case of Kafka)
    * @param content Received content
    * @return generated record as Row or <code>None</code> in case it couldn't be generated for some reason
    */
  def createRecord(meta: String, content: String): Option[Row] = None

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