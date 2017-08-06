package com.github.viyadb.spark.streaming.message

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.github.viyadb.spark.Configs.{DimensionConf, JobConf, MetricConf, ParseSpecConf}
import com.github.viyadb.spark.util.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class MessageFactory(config: JobConf) extends Serializable {

  @transient
  protected val parseSpec = config.table.realTime.parseSpec.getOrElse(new ParseSpecConf(""))

  @transient
  protected val messageSchema = getMessageSchema()

  @transient
  protected val indexedFields = messageSchema.fields.zipWithIndex

  @transient
  protected val fieldTimeFormats = parseTimeFormats()

  @transient
  protected val javaValueParser = new JavaValueParser(parseSpec.nullNumericAsZero, parseSpec.nullStringAsEmpty)

  protected def getColumnNames(): Seq[String] = {
    parseSpec.columns.getOrElse(
      config.table.dimensions.map(_.name) ++ config.table.metrics.map(_.name)
    )
  }

  /**
    * @return mapping between schema field indices and in incoming data (if we're talking about tabular data)
    */
  protected def getColumnIndices(): Array[Int] = {
    val extractCols = getColumnNames().zipWithIndex.toMap
    messageSchema.fields.map(field => extractCols.get(field.name).get)
  }

  /**
    * @return mapping between schema field names and paths used to extract fields from an hierarchical structure
    *         (like Json, Avro, etc)
    */
  protected def getFieldExtractPaths(): Option[Array[String]] = {
    parseSpec.fieldMapping
      .map(mapping => messageSchema.fields.map(f => mapping.get(f.name).get))
  }

  /**
    * @return time formatter suitable for every extracted field, or <code>None</code> if not available
    */
  protected def parseTimeFormats(): Array[Option[SimpleDateFormat]] = {
    messageSchema.fields.map { field =>
      config.table.dimensions.filter(d => d.name.eq(field.name) && d.isTimeType())
        .flatMap(_.format)
        .map(format => TimeUtil.strptime2JavaFormat(format))
        .headOption
    }
  }

  protected def maxValueType(max: Option[Long]): DataType = {
    max.getOrElse((Integer.MAX_VALUE - 1).toLong) match {
      case x if x < Int.MaxValue => IntegerType
      case _ => LongType
    }
  }

  protected def dimensionDataType(dim: DimensionConf): DataType = {
    dim.`type`.getOrElse("string") match {
      case "string" => StringType
      case "numeric" => maxValueType(dim.max)
      case "time" | "microtime" => TimestampType
      case _ => throw new IllegalArgumentException(s"Unknown dimension type: ${dim.`type`}")
    }
  }

  protected def metricDataType(metric: MetricConf): DataType = {
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
    * @return schema for the Message object that will be created in <code>createMessage()</code> method
    */
  protected def getMessageSchema(): StructType = {
    val column2Type = (
      config.table.dimensions.map(dim => (dim.name, StructField(dim.name, dimensionDataType(dim)))) ++
        config.table.metrics.filter(_.`type` != "count")
          .map(metric => (metric.name, StructField(metric.name, metricDataType(metric))))
      ).toMap

    StructType(getColumnNames().map(col => column2Type.get(col)).filter(_.nonEmpty).map(_.get))
  }

  protected def parseTimeValue(value: String, fieldIdx: Int): Timestamp = {
    fieldTimeFormats(fieldIdx).map(format => new Timestamp(format.parse(value).getTime())).getOrElse(
      // XXX: support default formats
      new Timestamp(value.toLong)
    )
  }

  /**
    * Parses string values according to schema, and create a message
    *
    * @param values String field values
    * @return message
    */
  protected def createMessage(values: Array[String]): Option[Row] = {
    Some(
      new Message(indexedFields.zip(values).map { case ((field, fieldIdx), value) =>
        field.dataType match {
          case IntegerType => value.toInt
          case LongType => value.toLong
          case DoubleType => value.toDouble
          case TimestampType => parseTimeValue(value, fieldIdx)
          case StringType => value
        }
      })
    )
  }

  /**
    * Casts values according to schema, and creates a message from them
    *
    * @param values Field values as Java objects
    * @return message
    */
  protected def createMessage(values: Array[_ <: Object]): Option[Row] = {
    Some(
      new Message(indexedFields.zip(values).map { case ((field, fieldIdx), value) =>
        field.dataType match {
          case IntegerType => javaValueParser.parseInt(value)
          case LongType => javaValueParser.parseLong(value)
          case DoubleType => javaValueParser.parseDouble(value)
          case TimestampType => javaValueParser.parseTimestamp(value).getOrElse(
            parseTimeValue(javaValueParser.parseString(value), fieldIdx)
          )
          case StringType => javaValueParser.parseString(value)
        }
      })
    )
  }

  /**
    * Create message from received string
    *
    * @param meta    Some metadata (topic in case of Kafka)
    * @param content Received content
    * @return generated message as Row or <code>None</code> in case it couldn't be generated for some reason
    */
  def createMessage(meta: String, content: String): Option[Row]

  /**
    * Create Spark data frame for RDD of messages
    *
    * @param rdd RDD of messages (rows)
    * @return Spark data frame
    */
  def createDataFrame(rdd: RDD[Row]): DataFrame = {
    SparkSession.builder().getOrCreate().createDataFrame(rdd, messageSchema)
  }
}

object MessageFactory {
  def create(config: JobConf): MessageFactory = {
    config.table.realTime.messageFactoryClass.map(c =>
      Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[MessageFactory]
    ).getOrElse(
      config.table.realTime.parseSpec.map(parseSpec =>
        parseSpec.format match {
          case "tsv" => new TsvMessageFactory(config)
          case "json" => new JsonMessageFactory(config)
          case _ => throw new IllegalArgumentException("Unsupported message format specified in parse spec!")
        }
      ).getOrElse(
        throw new IllegalArgumentException("Either messageFactoryClass or parseSpec must be specified!")
      )
    )
  }
}