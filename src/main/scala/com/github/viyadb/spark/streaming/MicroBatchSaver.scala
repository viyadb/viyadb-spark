package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.Configs.{JobConf, TableConf}
import com.github.viyadb.spark.batch.OutputSchema
import com.github.viyadb.spark.util.RDDMultipleTextOutputFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.Time
import org.apache.spark.util.LongAccumulator

/**
  * Saves micro-batch data frame as TSV files into separate directories per current batch period and micro-batch time.
  *
  * The folder structure is the following:
  * <pre>
  * &lt;deepStorePath&gt;/realtime/&lt;table name>&gt;dt=&lt;batch ID&gt;/mb=&lt;micro-batch ID&gt;
  * </pre>
  */
class MicroBatchSaver(jobConf: JobConf) extends Serializable {

  private val pathTracker: PathTracker = new PathTracker()

  private val recordCountAcc: Map[String, LongAccumulator] = jobConf.tableConfigs.map(tableConf =>
    (
      tableConf.name,
      SparkSession.builder().getOrCreate().sparkContext.longAccumulator(s"${tableConf.name} record count")
    )
  ).toMap

  /**
    * Saves table data frame to disk, and returns all written paths per table
    *
    * @return number of written records
    */
  def save(tableConf: TableConf, tableDf: DataFrame, time: Time): Long = {
    import tableDf.sqlContext.implicits._

    val outputSchema = new OutputSchema(tableConf)

    val periodMillis = jobConf.indexer.batch.batchDurationInMillis
    val timeColumn = jobConf.indexer.realTime.parseSpec.flatMap(_.timeColumn)
    val truncatedTime = if (timeColumn.isEmpty) {
      Some(Math.floor(time.milliseconds / periodMillis).toLong * periodMillis)
    } else {
      None
    }

    val targetPath = s"${jobConf.indexer.realtimePrefix}/${tableConf.name}"

    val countAcc = recordCountAcc(tableConf.name)

    tableDf.mapPartitions(partition =>
      partition.map(row => {
        countAcc.add(1)

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

    countAcc.value
  }

  def getAndResetWrittenPaths(): Map[String, Seq[String]] = {
    pathTracker.getPathsAndReset()
  }

  def getAndResetRecordsCount(): Map[String, Long] = {
    try {
      recordCountAcc.map { case (table, acc) => (table, acc.value.longValue()) }
    } finally {
      recordCountAcc.values.foreach(_.reset())
    }
  }
}