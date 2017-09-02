package com.github.viyadb.spark.streaming.record

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.viyadb.spark.Configs.JobConf
import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.Row

class JsonRecordFactory(config: JobConf) extends RecordFactory(config) {

  @transient
  private lazy val jsonPathConf = Configuration.builder()
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(util.EnumSet.of(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS))
    .build()

  @transient
  private lazy val jsonPaths = getColumnMapping().map(paths => paths.map(path => JsonPath.compile(path)))

  class SerializableTypeReference extends TypeReference[java.util.Map[String, Object]]
    with Serializable {}

  private val typeReference = new SerializableTypeReference()

  private val jsonMapper = new ObjectMapper()

  override def createRecord(meta: String, content: String): Option[Row] = {
    val doc = jsonMapper.readValue[java.util.Map[String, Object]](content, typeReference)

    Some(
      parseJavaObjects(
        jsonPaths.map(paths => paths.map(path =>
          path.read(doc.asInstanceOf[Object], jsonPathConf).asInstanceOf[Object]
        )).getOrElse(
          inputSchema.fields.map(f => doc.get(f.name))
        )
      )
    )
  }
}