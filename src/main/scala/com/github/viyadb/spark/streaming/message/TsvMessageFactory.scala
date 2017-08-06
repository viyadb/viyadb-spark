package com.github.viyadb.spark.streaming.message

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.Row

class TsvMessageFactory(config: JobConf) extends MessageFactory(config) {

  private val delimiter = parseSpec.delimiter.getOrElse("\t")

  private val columnIndices = getColumnIndices()

  override def createMessage(meta: String, content: String): Option[Row] = {
    val lines = content.split(delimiter)
    createMessage(columnIndices.map(idx => lines(idx)))
  }
}