package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.util.FileSystemUtil
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Processes micro-batch data produced by the streaming processes
  */
class BatchProcess(config: JobConf) extends Serializable with Logging {

  lazy protected val recordFormat = new BatchRecordFormat(config)

  lazy private val processor = config.table.batch.processorClass.map(c =>
    Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[Processor]
  ).getOrElse(new BatchProcessor(config))

  /**
    * Finds out what batches are not processed yet
    *
    * @return list of batch timestamps
    */
  private def batchesToProcess(): Seq[Long] = {
    val periodMillis = config.table.batch.batchDurationInMillis
    val currentBatch = Math.floor(System.currentTimeMillis / periodMillis).toLong * periodMillis

    def getBatchTimestamps(prefix: String): Seq[Long] = {
      FileSystemUtil.list(prefix).map(_.getPath.getName).filter(_.startsWith("dt="))
        .map(_.substring(3).toLong)
    }

    val processedBatches = getBatchTimestamps(config.batchPrefix()).toSet
    getBatchTimestamps(config.realtimePrefix())
      .filter(ts => !processedBatches.contains(ts) && (ts != currentBatch))
  }

  /**
    * Starts the batch processing
    *
    * @param spark Spark session
    */
  def start(spark: SparkSession): Unit = {
    batchesToProcess().par.foreach { ts =>
      val sourceDir = s"${config.realtimePrefix()}/dt=${ts}"
      val targetDir = s"${config.batchPrefix()}/dt=${ts}"
      logInfo(s"${sourceDir} => ${targetDir}")

      val df = recordFormat.loadDataFrame(spark, s"${sourceDir}/**")
      FileSystemUtil.delete(targetDir)

      processor.process(df)
        .rdd
        .map(recordFormat.toTsvLine(_))
        .saveAsTextFile(targetDir, classOf[GzipCodec])
    }
  }
}