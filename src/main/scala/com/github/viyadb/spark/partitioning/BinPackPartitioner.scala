package com.github.viyadb.spark.partitioning

import org.apache.spark.Partitioner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


/**
  * This partitioner tries to partition data to predefined number of equal parts in terms of rows number.
  *
  * First, it takes the data frame a containing representative data set, and calculates
  * partitioning scheme using greedy algorithm for the Bin Packing problem. This partition scheme
  * is then used in method getPartition when calculating target partition number.
  *
  * @param sampleData  Representative data set to be used when calculating partitioning scheme. The sample data set
  *                    must include all the keys that will come through this partitioner.
  * @param partitionBy Column to partition data set by (tip: this column can be added for partitioning purposes only,
  *                    then it can be removed prior to writing the data).
  * @param bucketsNum  Desired buckets number.
  */
class BinPackPartitioner(sampleData: DataFrame, partitionBy: Column, bucketsNum: Int) extends Partitioner {

  protected def calcPartitionScheme(): Map[Any, Int] = {
    val rowNums = sampleData.groupBy(partitionBy).agg(count(partitionBy)).collect().map(r => (r(0), r.getAs[Long](1)))

    BinPackAlgorithm.packBins(rowNums, bucketsNum).filter(_.nonEmpty)
      .zipWithIndex.flatMap { case (bins, index) =>
      bins.map { case (elems, _) =>
        (elems, index)
      }
    }.toMap
  }

  lazy val partitioner = new StaticPartitioner(calcPartitionScheme())

  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = partitioner.getPartition(key)
}
