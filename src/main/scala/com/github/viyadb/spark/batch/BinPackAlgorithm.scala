package com.github.viyadb.spark.batch

import scala.collection.mutable.ListBuffer

/**
  * Greedy implementation of BinPack algorithm
  */
object BinPackAlgorithm {

  case class Bin[A](elements: ListBuffer[(A, Long)] = new ListBuffer[(A, Long)],
                    var total: Long = 0) extends Serializable

  /**
    * @param elemsByCount List of tuples: (element, weight).
    * @param binsNum      Target number of bins to create.
    * @return sequence of Bin objects containing the needed partitioning. The number of resulted bins can be smaller
    *         than the requested number in case it's impossible to split equally.
    */
  def packBins[A](elemsByCount: Seq[(A, Long)], binsNum: Int): Array[Bin[A]] = {
    val bins = Array.fill[Bin[A]](binsNum)(Bin[A]())
    val maxBinSize = elemsByCount.map(_._2).sum / binsNum

    elemsByCount.sortBy(-_._2).foreach { case (element, weight) =>
      val targetBin = bins.find(bin => (bin.total + weight <= maxBinSize)).getOrElse(bins.minBy(_.total))
      targetBin.elements += Tuple2(element, weight)
      targetBin.total += weight
    }
    bins
  }
}
