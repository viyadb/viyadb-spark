package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.batch.OutputFormat
import com.github.viyadb.spark.util.RDDMultipleTextOutputFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.Time

/**
  * Saves micro-batch data frame as TSV files into separate directories per current batch period and micro-batch time.
  */
class MicroBatchSaver(pathAccumulator: PathAccumulator, config: JobConf) extends Serializable {

  private val outputFormat = new OutputFormat(config)

  private lazy val pathTracker = new PathTracker(pathAccumulator, config.realtimePrefix())

  def save(df: DataFrame, time: Time): Unit = {
    import df.sqlContext.implicits._

    val periodMillis = config.table.batch.batchDurationInMillis
    val truncatedTime = if (config.table.timeColumn.isEmpty) {
      Some(Math.floor(time.milliseconds / periodMillis).toLong * periodMillis)
    } else {
      None
    }

    df.mapPartitions(partition =>
      partition.map(row => {
        val targetDt = truncatedTime.getOrElse(
          Math.floor(
            row.getAs[java.util.Date](config.table.timeColumn.get).getTime / periodMillis)
            .toLong * periodMillis
        )

        val path = s"dt=${targetDt}/mb=${time.milliseconds}"
        // Track written path so it will be reported along with the micro-batch info by the notifier
        pathTracker.trackPath(path)

        (path, outputFormat.toTsvLine(row))
      })
    )
      .rdd
      .saveAsHadoopFile(config.realtimePrefix(), classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
  }
}