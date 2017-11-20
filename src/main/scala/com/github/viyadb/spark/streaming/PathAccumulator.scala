package com.github.viyadb.spark.streaming

import java.util.Collections

import org.apache.spark.util.AccumulatorV2

class PathAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
  private val acc = Collections.synchronizedSet(new java.util.HashSet[String]())

  override def isZero: Boolean = acc.isEmpty

  override def copy() = {
    val newAcc = new PathAccumulator()
    newAcc.acc.addAll(acc)
    newAcc
  }

  override def reset() = acc.clear()

  override def add(path: String) = acc.add(path)

  override def merge(other: AccumulatorV2[String, java.util.Set[String]]) = {
    acc.addAll(other.value)
  }

  override def value: java.util.Set[String] = acc
}
