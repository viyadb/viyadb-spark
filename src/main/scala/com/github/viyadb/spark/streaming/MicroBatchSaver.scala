package com.github.viyadb.spark.streaming

import java.sql.Timestamp

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.util.RDDMultipleTextOutputFormat
import com.github.viyadb.spark.util.TsvUtils._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Time
import org.joda.time.Period

/**
  * Saves micro-batch data frame as TSV files into separate directories per current batch period and micro-batch time.
  */
class MicroBatchSaver(config: JobConf) extends Serializable {

  private val outputPath = s"${config.table.deepStorePath}/realtime"

  def save(df: DataFrame, time: Time): Unit = {
    import df.sqlContext.implicits._

    val periodMillis = config.table.batch.batchDuration.getOrElse(Period.days(1)).
      toStandardSeconds.getSeconds * 1000L

    df.map(row => {
      val timestamp = config.table.timeColumn.map(timeColumn =>
        row.getAs[Timestamp](timeColumn.name).getTime).getOrElse(time.milliseconds)

      val truncatedTime = Math.floor(timestamp / periodMillis).toLong * periodMillis

      (s"dt=${truncatedTime}/mb=${time.milliseconds}", row.toTsv())
    })
      .rdd
      .saveAsHadoopFile(outputPath, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
  }
}