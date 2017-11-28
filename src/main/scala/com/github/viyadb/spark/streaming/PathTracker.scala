package com.github.viyadb.spark.streaming

import com.github.viyadb.spark.util.SetAccumulator
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**
  * Uses Spark's accumulator to track written paths
  */
class PathTracker extends Serializable {

  private val seenPaths = scala.collection.mutable.Set[String]()
  private val accumulator = createAccumulator()

  def createAccumulator(): SetAccumulator[(String, String)] = {
    val acc = new SetAccumulator[(String, String)]()
    SparkSession.builder().getOrCreate().sparkContext.register(acc, "Written paths")
    acc
  }

  def trackPath(table: String, path: String): Unit = {
    // Only invoke accumulator if locally we are seing a new value:
    if (seenPaths.add(path)) {
      accumulator.add((table, path))
    }
  }

  /**
    * Returns all written paths per table: { table -> [path1, path2, ...], ... }
    */
  def getPathsAndReset(): Map[String, Seq[String]] = {
    try {
      return accumulator.value.asScala.toSeq.groupBy(_._1).mapValues(_.map(_._2))
    } finally {
      accumulator.reset()
    }
  }
}
