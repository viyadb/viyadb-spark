package com.github.viyadb.spark.streaming.parser

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.viyadb.spark.Configs.JobConf
import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

class JsonRecordParser(jobConf: JobConf) extends RecordParser(jobConf) {

  @transient
  private lazy val jsonPathConf = Configuration.builder()
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(util.EnumSet.of(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS))
    .build()

  @transient
  private lazy val jsonPaths = parseSpec.fieldMapping
    .map(mapping => inputSchema.fields.map(f => mapping.get(f.name).get))
    .map(paths => paths.map(path => JsonPath.compile(path)))

  class SerializableTypeReference extends TypeReference[java.util.Map[String, Object]]
    with Serializable {}

  private val typeReference = new SerializableTypeReference()

  private val jsonMapper = new ObjectMapper()

  private val javaValueParser = new JavaValueParser(
    parseSpec.nullNumericAsZero.getOrElse(true), parseSpec.nullStringAsEmpty.getOrElse(true))

  /**
    * Parses record from Java types values
    *
    * @param values Java types values
    * @return record
    */
  protected def parseJavaObjects(values: Array[_ <: Object]): Record = {
    new Record(indexedInputSchema.map { case (field, fieldIdx) =>
      val value = values(fieldIdx)
      field.dataType match {
        case ByteType => javaValueParser.parseByte(value)
        case ShortType => javaValueParser.parseShort(value)
        case IntegerType => javaValueParser.parseInt(value)
        case LongType => javaValueParser.parseLong(value)
        case FloatType => javaValueParser.parseFloat(value)
        case DoubleType => javaValueParser.parseDouble(value)
        case TimestampType => javaValueParser.parseTimestamp(value).getOrElse(
          parseTime(javaValueParser.parseString(value), fieldIdx)
        )
        case StringType => javaValueParser.parseString(value)
      }
    })
  }

  override def parseRecord(topic: String, record: String): Option[Record] = {
    Try {
      val doc = jsonMapper.readValue[java.util.Map[String, Object]](record, typeReference)
      parseJavaObjects(
        jsonPaths.map(paths => paths.map(path =>
          path.read(doc.asInstanceOf[Object], jsonPathConf).asInstanceOf[Object]
        )).getOrElse(
          inputSchema.fields.map(f => doc.get(f.name))
        )
      )
    } match {
      case Success(v) => Some(v)
      case Failure(e) => {
        logWarning(e.getMessage)
        None
      }
    }
  }
}
