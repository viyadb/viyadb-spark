package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{IndexerConf, PartitionConf, TableConf}
import com.github.viyadb.spark.batch.BatchProcess.BatchTableInfo
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.util.{FileSystemUtil, RDDMultipleTextOutputFormat}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Processes micro-batch table data produced by the streaming processes
  */
class TableBatchProcess(indexerConf: IndexerConf, tableConf: TableConf) extends Serializable with Logging {

  lazy protected val microBatchLoader = new MicroBatchLoader(tableConf)

  lazy private val processor = indexerConf.realTime.processorClass.map(c =>
    Class.forName(c).getDeclaredConstructor(classOf[TableConf]).newInstance(tableConf).asInstanceOf[Processor]
  ).getOrElse(new BatchProcessor(tableConf))

  lazy private val outputSchema = new OutputSchema(tableConf)

  private def previousBatch(ts: Long): Option[Long] = {
    val periodMillis = indexerConf.batch.batchDurationInMillis
    Some(Math.floor(ts - 1 / periodMillis).toLong * periodMillis)
      .filter(ts => FileSystemUtil.exists(s"${indexerConf.batchPrefix}/${tableConf.name}/dt=$ts"))
  }

  /**
    * Processes real-time data for a table, and writes it in an unpartitioned way
    */
  protected def processBatch(batchId: Long, targetPath: String): Unit = {
    val sourcePath = s"${indexerConf.realtimePrefix}/${tableConf.name}/dt=$batchId/**/*.gz"
    logInfo(s"Processing $sourcePath => $targetPath")

    val df = microBatchLoader.loadDataFrame(sourcePath)

    val unionDf = previousBatch(batchId).map { prevPatch =>
      val prevPatchPath = s"${indexerConf.batchPrefix}/${tableConf.name}/dt=$prevPatch/**/*.gz"
      logInfo(s"Loading previous batch $prevPatchPath")
      microBatchLoader.loadDataFrame(prevPatchPath)
    }
      .map(_.union(df))
      .getOrElse(df)

    FileSystemUtil.delete(targetPath)

    processor.process(unionDf).rdd
      .mapPartitions(partition => partition.map(outputSchema.toTsvLine(_)))
      .saveAsTextFile(targetPath, classOf[GzipCodec])
  }

  protected def calculatePartitoins(df: DataFrame, partitionColumn: String, numPartitions: Int): Map[Any, Int] = {
    logInfo(s"Calculating partitioining")
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
  protected def partitionBatch(batchId: Long,
                               sourcePath: String, targetPath: String,
                               partitionConf: PartitionConf): Map[Any, Int] = {

    logInfo(s"Partitioning $sourcePath => $targetPath")
    var df = microBatchLoader.loadDataFrame(sourcePath)

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

    df = Seq(tableConf.sortColumns, indexerConf.batch.sortColumns).find(_.nonEmpty).flatten.map(sortColumns =>
      df.sortWithinPartitions(sortColumns.map(col): _*)
    ).getOrElse(df)

    df.rdd
      .mapPartitions(p =>
        p.map(row => (s"part=${getPartition(row.getAs[Any](partColumn))}", outputSchema.toTsvLine(row, dropColumns))))
      .saveAsHadoopFile(targetPath, classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])

    partitions
  }

  /**
    * Starts the batch processing for a table
    */
  def start(batchId: Long): BatchTableInfo = {
    val targetPath = s"${indexerConf.batchPrefix}/${tableConf.name}/dt=$batchId"
    val partitionConf = Seq(tableConf.partitioning, indexerConf.batch.partitioning)
      .find(_.nonEmpty).flatten

    val partitioning = if (partitionConf.isEmpty) {
      processBatch(batchId, targetPath)
      None
    } else {
      val tmpPath = s"${indexerConf.batchPrefix}/${tableConf.name}/dt=$batchId/_unpart"
      processBatch(batchId, tmpPath)
      try {
        Some(partitionBatch(batchId, s"$tmpPath/*.gz", targetPath, partitionConf.get))
      } finally {
        FileSystemUtil.delete(tmpPath)
      }
    }
    BatchTableInfo(
      paths = Seq(targetPath),
      partitioning = partitioning,
      partitionConf = partitionConf
    )
  }
}
