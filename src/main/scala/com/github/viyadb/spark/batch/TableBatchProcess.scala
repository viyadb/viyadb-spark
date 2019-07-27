package com.github.viyadb.spark.batch

import com.github.viyadb.spark.Configs.{IndexerConf, PartitionConf, TableConf}
import com.github.viyadb.spark.batch.BatchProcess.BatchTableInfo
import com.github.viyadb.spark.processing.Processor
import com.github.viyadb.spark.util.{FileSystemUtil, RDDMultipleTextOutputFormat}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Processes micro-batch table data produced by the streaming processes
  */
class TableBatchProcess(indexerConf: IndexerConf, tableConf: TableConf) extends Serializable with Logging {

  lazy protected val microBatchLoader = new MicroBatchLoader(tableConf)

  lazy private val processor = indexerConf.realTime.processorClass.map(c =>
    Class.forName(c).getDeclaredConstructor(classOf[TableConf]).newInstance(tableConf).asInstanceOf[Processor]
  ).getOrElse(new BatchProcessor(tableConf))

  lazy private val outputSchema = new OutputSchema(tableConf)

  private val recordCountAcc = SparkSession.builder().getOrCreate().sparkContext
    .longAccumulator(s"${tableConf.name} record count")

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

  protected def calculatePartitions(df: DataFrame, partitionColumn: String, numPartitions: Int): Seq[Int] = {
    logInfo(s"Calculating partitioining")

    val rowStats = df.groupBy(partitionColumn).agg(count(lit(1))).collect()
      .map(r => (r.getAs[Int](0), r.getAs[Long](1)))

    val valueToPartition = BinPackAlgorithm.packBins(rowStats, numPartitions)
      .zipWithIndex.flatMap { case (bin, index) => bin.elements.map(key => (key._1, index)) }
      .toMap

    val usedPartitions = valueToPartition.values.toSet
    val unusedPartitions = (0 until numPartitions).filter(!usedPartitions.contains(_)).iterator

    (0 until numPartitions).map(value => valueToPartition.getOrElse(value, unusedPartitions.next()))
  }

  /**
    * Repartition processed data
    */
  protected def partitionBatch(batchId: Long,
                               sourcePath: String, targetPath: String,
                               partitionConf: PartitionConf): Seq[Int] = {

    logInfo(s"Partitioning $sourcePath => $targetPath")

    if (partitionConf.columns.exists(col => !tableConf.dimensions.exists(dim => dim.name == col))) {
      throw new IllegalArgumentException("Only table dimensions can be used in partition key")
    }

    val partColumn = "__viyadb_part_col"

    var df = microBatchLoader.loadDataFrame(sourcePath)
      .withColumn(partColumn,
        pmod(crc32(concat(partitionConf.columns.map(col): _*)), lit(partitionConf.partitions)).cast(IntegerType))

    val partitions = calculatePartitions(df, partColumn, partitionConf.partitions)

    val getPartitionUdf = udf((value: Int) => partitions(value))

    val partNumColumn = "__viyadb_part_num"
    df = df.withColumn(partNumColumn, getPartitionUdf(col(partColumn)))
      .repartition(col(partNumColumn))
      .drop(partNumColumn)

    df = Seq(tableConf.sortColumns, indexerConf.batch.sortColumns).find(_.nonEmpty).flatten.map(sortColumns =>
      df.sortWithinPartitions(sortColumns.map(col): _*)
    ).getOrElse(df)

    df.rdd
      .mapPartitions { partition =>
        partition.map { row =>
          recordCountAcc.add(1)
          (s"part=${partitions(row.getAs[Int](partColumn))}", outputSchema.toTsvLine(row, 1))
        }
      }
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
      columns = outputSchema.columns,
      partitioning = partitioning,
      partitionConf = partitionConf,
      recordCount = recordCountAcc.value
    )
  }
}
