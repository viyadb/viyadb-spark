package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{JobConf, PartitionConf}
import com.github.viyadb.spark.batch.BatchProcess.BatchInfo
import com.github.viyadb.spark.notifications.Notifier
import com.github.viyadb.spark.streaming.StreamingProcess.MicroBatchInfo
import com.timgroup.statsd.StatsDClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Processes micro-batch data produced by the streaming processes
  */
class BatchProcess(jobConf: JobConf) extends Serializable with Logging {

  lazy private val notifier: Notifier[BatchInfo] = Notifier.create(jobConf, jobConf.indexer.batch.notifier)

  @transient
  lazy protected val statsd: Option[StatsDClient] = jobConf.indexer.statsd.map(_.createClient)

  /**
    * Find unprocessed real-time micro-batches from the two notification channels
    * that correspond to real-time and batch processes.
    *
    * @return tuple of format: (Batch ID, Micro batches list, Table names list)
    */
  protected def unprocessedBatches(): Seq[(Long, Seq[Long], Seq[String])] = {
    val periodMillis = jobConf.indexer.batch.batchDurationInMillis
    val currentBatch = Math.floor(System.currentTimeMillis / periodMillis).toLong * periodMillis

    // Read last processed batch
    val lastBatch = notifier.lastMessage.map(_.id).getOrElse(0L)

    def extractBatchIdFromPath(path: String): Long = {
      val dtSegmentPattern = ".*/dt=(\\d+)/.*".r
      path match {
        case dtSegmentPattern(c) => c.toLong
        case _ => throw new IllegalArgumentException(s"Can't extract batch ID from path: $path")
      }
    }

    def isUnprocessedBatch(batchId: Long): Boolean = {
      batchId > lastBatch && batchId != currentBatch
    }

    // Read all notifications send by the real-time process
    val realTimeNotifier = Notifier.create[MicroBatchInfo](jobConf, jobConf.indexer.realTime.notifier)
    realTimeNotifier.allMessages.flatMap { mbInfo =>
      mbInfo.tables.flatMap { case (tableName, tableInfo) =>
        tableInfo.paths.map(extractBatchIdFromPath).map(batchId => (batchId, mbInfo.id, tableName))
      }
    }.filter(v => isUnprocessedBatch(v._1))
      .groupBy(_._1).mapValues(v => (v.map(_._2).distinct.sorted, v.map(_._3).distinct))
      .map(v => (v._1, v._2._1, v._2._2))
      .toSeq.sortBy(_._1)
  }

  protected def processTable(tableName: String, batchId: Long): BatchProcess.BatchTableInfo = {
    val startTime = System.currentTimeMillis

    val tableConf = jobConf.tableConfigs.find(conf => conf.name == tableName).get
    val tableInfo = new TableBatchProcess(jobConf.indexer, tableConf).start(batchId)

    statsd.foreach(client => {
      client.recordExecutionTime(s"batch.tables.$tableName.process_time", System.currentTimeMillis - startTime)
      client.count(s"batch.tables.$tableName.saved_records", tableInfo.recordCount)
    })

    tableInfo
  }

  /**
    * Starts the batch processing
    */
  def start(spark: SparkSession): Unit = {
    unprocessedBatches().map { case (batchId, microBatches, tables) =>
      val tablesInfo = tables.par.map(tableName => (tableName, processTable(tableName, batchId)))
        .seq.toMap
      BatchInfo(id = batchId, tables = tablesInfo, microBatches = microBatches)
    }.foreach(batchInfo => notifier.send(batchInfo.id, batchInfo))
  }
}

object BatchProcess {

  case class BatchTableInfo(paths: Seq[String],
                            columns: Seq[String],
                            partitioning: Option[Seq[Int]],
                            partitionConf: Option[PartitionConf],
                            recordCount: Long) extends Serializable

  case class BatchInfo(id: Long,
                       tables: Map[String, BatchTableInfo],
                       microBatches: Seq[Long]) extends Serializable

}