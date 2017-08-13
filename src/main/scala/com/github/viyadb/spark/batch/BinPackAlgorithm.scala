package com.github.viyadb.spark.batch

import scala.collection.mutable.ListBuffer

/**
  * Greedy implementation of BinPack algorithm
  */
object BinPackAlgorithm {

  /**
    * @param elemsByCount List of tuples: (element, weight).
    * @param binsNum      Target number of bins to create.
    * @return sequence of Bin objects containing the needed partitioning. The number of resulted bins can be smaller
    *         than the requested number in case it's impossible to split equally.
    */
  def packBins(elemsByCount: Seq[(Any, Long)], binsNum: Int): Array[Seq[(Any, Long)]] = {
    case class Bin(keys: ListBuffer[(Any, Long)] = new ListBuffer, var total: Long = 0)

    val bins = Array.fill[Bin](binsNum)(Bin())

    val maxBinSize = elemsByCount.map(_._2).sum / binsNum

    elemsByCount.sortBy(-_._2).foreach { case (element: Any, weight) =>
      val targetBin = bins.find(bin => (bin.total + weight <= maxBinSize)).getOrElse(bins.minBy(_.total))
      targetBin.keys += Tuple2(element, weight)
      targetBin.total += weight
    }

    bins.map(_.keys.toSeq)
  }
}
