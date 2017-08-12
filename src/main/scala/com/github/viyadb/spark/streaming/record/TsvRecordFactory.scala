package com.github.viyadb.spark.streaming.record

import com.github.viyadb.spark.Configs.JobConf
import org.apache.spark.sql.Row

class TsvRecordFactory(config: JobConf) extends RecordFactory(config) {

  private val delimiter = config.table.realTime.parseSpec.flatMap(_.delimiter).getOrElse("\t")

  override def createRecord(meta: String, content: String): Option[Row] = {
    Some(
      parseInputRow(content.split(delimiter))
    )
  }
}