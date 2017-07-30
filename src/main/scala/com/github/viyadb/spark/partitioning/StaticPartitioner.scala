package com.github.viyadb.spark.partitioning

import org.apache.spark.Partitioner

/**
  * Spark partitioner that uses static partitioning scheme
  *
  * @param scheme Mapping: key -> target partition number
  */
class StaticPartitioner(scheme: Map[Any, Int]) extends Partitioner {

  override def numPartitions: Int = scheme.size

  override def getPartition(key: Any): Int = scheme.get(key).get
}
