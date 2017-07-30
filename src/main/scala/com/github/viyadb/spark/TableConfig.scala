package com.github.viyadb.spark

import com.github.viyadb.spark.util.JodaSerializers
import org.joda.time.{Duration, Interval}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
  * Table configuration classes and parser utility
  */
object TableConfig {

  case class Kafka(topics: Seq[String], brokers: Seq[String], offsetPath: Option[String]) extends Serializable

  case class ParseSpec(format: String, columns: Option[Seq[String]]) extends Serializable

  case class TimeColumn(name: String, format: Option[String]) extends Serializable

  case class RealTime(kafka: Option[Kafka], parseSpec: Option[ParseSpec],
                      windowDuration: Option[Duration] = Some(Duration.standardMinutes(5)),
                      messageFactoryClass: Option[String],
                      outputPath: String) extends Serializable

  case class Dimension(name: String) extends Serializable

  case class Metric(name: String, `type`: String = "string", format: Option[String],
                    granularity: Option[String]) extends Serializable

  case class Batch(batchDuration: Option[Duration] = Some(Duration.standardDays(1)),
                   keepInterval: Option[Interval],
                   timeColumn: Option[TimeColumn] = Some(TimeColumn("timestamp", None))) extends Serializable

  case class Table(realTime: RealTime,
                   batch: Batch,
                   processorClass: Option[String],
                   dimensions: Seq[Dimension],
                   metrics: Seq[Metric]) extends Serializable

  def parse(str: String): Table = {
    implicit val formats = DefaultFormats ++ JodaSerializers.all
    JsonMethods.parse(str).extract[Table]
  }
}
