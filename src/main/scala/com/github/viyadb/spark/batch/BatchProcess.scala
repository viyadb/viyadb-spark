package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{JobConf, PartitionConf}
import com.github.viyadb.spark.batch.BatchProcess.BatchInfo
import com.github.viyadb.spark.notifications.Notifier
import com.github.viyadb.spark.streaming.StreamingProcess.MicroBatchInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Processes micro-batch data produced by the streaming processes
  */
class BatchProcess(jobConf: JobConf) extends Serializable with Logging {

  lazy private val notifier: Notifier[BatchInfo] = Notifier.create(jobConf.indexer.batch.notifier)

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
    val realTimeNotifier = Notifier.create[MicroBatchInfo](jobConf.indexer.realTime.notifier)
    realTimeNotifier.allMessages.flatMap { mbInfo =>
      mbInfo.tables.flatMap { case (tableName, tableInfo) =>
        tableInfo.paths.map(extractBatchIdFromPath).map(batchId => (batchId, mbInfo.id, tableName))
      }
    }.filter(v => isUnprocessedBatch(v._1))
      .groupBy(_._1).mapValues(v => (v.map(_._2).distinct.sorted, v.map(_._3).distinct))
      .map(v => (v._1, v._2._1, v._2._2))
      .toSeq.sortBy(_._1)
  }

  /**
    * Starts the batch processing
    */
  def start(spark: SparkSession): Unit = {
    unprocessedBatches().map { case (batchId, microBatches, tables) =>
      val tablesInfo = tables.par.map { tableName =>
        (tableName, new TableBatchProcess(jobConf.indexer, jobConf.tableConfigs.find(conf => conf.name == tableName).get)
          .start(batchId))
      }.seq.toMap
      BatchInfo(id = batchId, tables = tablesInfo, microBatches = microBatches)
    }.foreach(batchInfo => notifier.send(batchInfo.id, batchInfo))
  }
}

object BatchProcess {

  case class BatchTableInfo(paths: Seq[String],
                            partitioning: Option[Map[Any, Int]],
                            partitionConf: Option[PartitionConf]) extends Serializable

  case class BatchInfo(id: Long,
                       tables: Map[String, BatchTableInfo],
                       microBatches: Seq[Long]) extends Serializable

}