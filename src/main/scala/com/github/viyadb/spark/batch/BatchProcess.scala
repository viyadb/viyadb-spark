package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{JobConf, PartitionConf}
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.util.{FileSystemUtil, RDDMultipleTextOutputFormat}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/**
  * Processes micro-batch data produced by the streaming processes
  */
class BatchProcess(config: JobConf) extends Serializable with Logging {

  lazy protected val recordFormat = new BatchRecordFormat(config)

  lazy private val processor = config.table.batch.processorClass.map(c =>
    Class.forName(c).getDeclaredConstructor(classOf[JobConf]).newInstance(config).asInstanceOf[Processor]
  ).getOrElse(new BatchProcessor(config))

  private def nextUnprocessedBatch: Option[Long] = {
    val periodMillis = config.table.batch.batchDurationInMillis
    val currentBatch = Math.floor(System.currentTimeMillis / periodMillis).toLong * periodMillis

    def getBatchTimestamps(prefix: String): Seq[Long] = {
      FileSystemUtil.list(prefix).map(_.getPath.getName).filter(_.startsWith("dt="))
        .map(_.substring(3).toLong)
    }

    val realtimeBatches = getBatchTimestamps(config.realtimePrefix)
    val processedBatches = getBatchTimestamps(config.batchPrefix)
    if (processedBatches.isEmpty) {
      realtimeBatches.sorted.headOption
    } else {
      realtimeBatches.filter(ts => (ts > realtimeBatches.max) && (ts != currentBatch)).sorted.headOption
    }
  }

  private def previousBatch(ts: Long): Option[Long] = {
    val periodMillis = config.table.batch.batchDurationInMillis
    Some(Math.floor(ts - 1 / periodMillis).toLong * periodMillis)
      .filter(ts => FileSystemUtil.exists(s"${config.batchPrefix}/dt=${ts}"))
  }

  /**
    * Processes real-time data, and writes it in an unpartitioned way
    */
  protected def processBatch(spark: SparkSession, batch: Long, targetPath: String): Unit = {
    val sourcePath = s"${config.realtimePrefix}/dt=${batch}/**/*.gz"
    logInfo(s"Processing ${sourcePath} => ${targetPath}")

    val df = recordFormat.loadDataFrame(spark, sourcePath)

    val unionDf = previousBatch(batch).map { prevPatch =>
      val prevPatchPath = s"${config.batchPrefix}/dt=${prevPatch}/**/*.gz"
      logInfo(s"Loading previous batch ${prevPatchPath}")
      recordFormat.loadDataFrame(spark, prevPatchPath)
    }
      .map(_.union(df))
      .getOrElse(df)

    FileSystemUtil.delete(targetPath)

    processor.process(unionDf).rdd
      .mapPartitions(partition => partition.map(recordFormat.toTsvLine(_)))
      .saveAsTextFile(targetPath, classOf[GzipCodec])
  }

  protected def calculatePartitoins(df: DataFrame, partitionColumn: String, numPartitions: Int) = {
    val rowStats = df.groupBy(partitionColumn).agg(count(lit(1))).collect().map(r => (r(0), r.getAs[Long](1)))

    BinPackAlgorithm.packBins(rowStats, numPartitions).filter(_.nonEmpty)
      .zipWithIndex.flatMap { case (bins, index) =>
      bins.map { case (elems, _) =>
        (elems, index)
      }
    }.toMap
  }

  /**
    * Repartition processed data
    */
  protected def partitionBatch(spark: SparkSession, batch: Long,
                               sourcePath: String, targetPath: String,
                               partitionConf: PartitionConf): Map[Any, Int] = {

    logInfo(s"Partitioning ${sourcePath} => ${targetPath}")
    var df = recordFormat.loadDataFrame(spark, sourcePath)

    val hashColumn = partitionConf.hashColumn.getOrElse(false)
    val partColumn = if (hashColumn) "__viyadb_part_col" else partitionConf.column
    if (hashColumn) {
      df = df.withColumn(partColumn, pmod(crc32(col(partitionConf.column)), lit(partitionConf.numPartitions)))
    }
    val dropColumns = if (hashColumn) 1 else 0
    val partitions = calculatePartitoins(df, partColumn, partitionConf.numPartitions)

    def getPartition(value: Any) = partitions.getOrElse(value, 0)

    val getPartitionUdf = udf((value: Any) => getPartition(value))

    val partNumColumn = "__viyadb_part_num"
    df = df.withColumn(partNumColumn, getPartitionUdf(col(partColumn)))
      .repartition(col(partNumColumn))
      .drop(partNumColumn)

    df = config.table.batch.sortColumns.map(sortColumns =>
      df.sortWithinPartitions(sortColumns.map(col): _*)
    ).getOrElse(df)

    df.rdd
      .mapPartitions(p =>
        p.map(row => (s"part=${getPartition(row.getAs[Any](partColumn))}", recordFormat.toTsvLine(row, dropColumns))))
      .saveAsHadoopFile(targetPath, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])

    partitions
  }

  /**
    * Save partitionining scheme to Consul
    */
  def savePartitions(partitions: Map[Any, Int], batch: Long) = {
    val key = s"${config.consulPrefix.stripSuffix("/")}/tables/${config.table.name}/partitions/${batch}"

    implicit val formats = DefaultFormats
    config.consulClient.kvPut(key, write(partitions))

    // TODO: remove old entries
  }

  /**
    * Starts the batch processing
    *
    * @param spark Spark session
    */
  def start(spark: SparkSession): Unit = {
    nextUnprocessedBatch.foreach { batch =>
      val targetPath = s"${config.batchPrefix}/dt=${batch}"

      if (config.table.batch.partitioning.isEmpty) {
        processBatch(spark, batch, targetPath)
      } else {
        val tmpPath = s"${config.batchPrefix}/dt=${batch}/_unpart"
        processBatch(spark, batch, tmpPath)

        val partitions = partitionBatch(spark, batch, s"${tmpPath}/*.gz", targetPath, config.table.batch.partitioning.get)
        savePartitions(partitions, batch)

        FileSystemUtil.delete(tmpPath)
      }
    }
  }
}
