package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.record.RecordFormat
import com.github.viyadb.spark.util.RDDMultipleTextOutputFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Time

/**
  * Saves micro-batch data frame as TSV files into separate directories per current batch period and micro-batch time.
  */
class MicroBatchSaver(config: JobConf, recordFormat: RecordFormat) extends Serializable {

  def save(df: DataFrame, time: Time): Unit = {
    import df.sqlContext.implicits._

    val periodMillis = config.table.batch.batchDurationInMillis
    val truncatedTime = if (config.table.timeColumn.isEmpty) {
      Some(Math.floor(time.milliseconds / periodMillis).toLong * periodMillis)
    } else {
      None
    }

    df.map(row => {
      val targetDt = truncatedTime.getOrElse(
        Math.floor(
          row.getAs[java.util.Date](config.table.timeColumn.get.name).getTime / periodMillis)
          .toLong * periodMillis
      )
      (s"dt=${targetDt}/mb=${time.milliseconds}", recordFormat.toTsvLine(row))
    })
      .rdd
      .saveAsHadoopFile(config.realtimePrefix(), classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
  }
}