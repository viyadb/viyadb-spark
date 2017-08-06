package com.github.viyadb.spark.streaming.message

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.viyadb.spark.Configs.JobConf
import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.spark.sql.Row

class JsonMessageFactory(config: JobConf) extends MessageFactory(config) {

  @transient
  private val jsonPathConf = Configuration.builder()
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(util.EnumSet.of(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS))
    .build()

  @transient
  private val jsonMapper = new ObjectMapper()

  @transient
  private val jsonPaths = getFieldExtractPaths().map(paths => paths.map(path => JsonPath.compile(path)))

  @transient
  private val typeReference = new TypeReference[java.util.Map[String, Object]]() {}

  override def createMessage(meta: String, content: String): Option[Row] = {
    val doc = jsonMapper.readValue[java.util.Map[String, Object]](content, typeReference)

    createMessage(
      jsonPaths.map(paths => paths.map(path =>
        path.read(doc.asInstanceOf[Object], jsonPathConf).asInstanceOf[Object]
      )).getOrElse(
        messageSchema.fields.map(f => doc.get(f.name))
      )
    )
  }
}