package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.{IndexerConf, TableConf}
import com.github.viyadb.spark.batch.OutputSchema
import com.github.viyadb.spark.util.RDDMultipleTextOutputFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Time

/**
  * Saves micro-batch data frame as TSV files into separate directories per current batch period and micro-batch time.
  *
  * The folder structure is the following:
  * <pre>
  * &lt;deepStorePath&gt;/realtime/&lt;table name>&gt;dt=&lt;batch ID&gt;/mb=&lt;micro-batch ID&gt;
  * </pre>
  */
class MicroBatchSaver(indexerConf: IndexerConf) extends Serializable {

  private val pathTracker = new PathTracker()

  /**
    * Saves table data frame to disk, and returns all written paths per table
    */
  def save(tableConf: TableConf, tableDf: DataFrame, time: Time): Unit = {
    import tableDf.sqlContext.implicits._

    val outputSchema = new OutputSchema(tableConf)

    val periodMillis = indexerConf.batch.batchDurationInMillis
    val timeColumn = indexerConf.realTime.parseSpec.flatMap(_.timeColumn)
    val truncatedTime = if (timeColumn.isEmpty) {
      Some(Math.floor(time.milliseconds / periodMillis).toLong * periodMillis)
    } else {
      None
    }

    val targetPath = s"${indexerConf.realtimePrefix}/${tableConf.name}"

    tableDf.mapPartitions(partition =>
      partition.map(row => {
        // Calculate bucket name on the target batch time:
        val targetDt = truncatedTime.getOrElse(
          Math.floor(
            row.getAs[java.util.Date](timeColumn.get).getTime / periodMillis).toLong * periodMillis
        )

        val path = s"dt=$targetDt/mb=${time.milliseconds}"

        // Track written path so it will be reported along with the micro-batch info by the notifier
        pathTracker.trackPath(tableConf.name, s"$targetPath/$path")

        (path, outputSchema.toTsvLine(row))
      })
    )
      .rdd
      .saveAsHadoopFile(targetPath, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
  }

  def getWrittenPaths(): Map[String, Seq[String]] = {
    pathTracker.getPathsAndReset()
  }
}