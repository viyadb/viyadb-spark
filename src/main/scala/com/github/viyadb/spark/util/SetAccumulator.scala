package com.github.viyadb.spark.util

import java.util.Collections

import org.apache.spark.util.AccumulatorV2

class SetAccumulator[A] extends AccumulatorV2[A, java.util.Set[A]] {
  private val acc = Collections.synchronizedSet(new java.util.HashSet[A]())

  override def isZero: Boolean = acc.isEmpty

  override def copy() = {
    val newAcc = new SetAccumulator[A]
    newAcc.acc.addAll(acc)
    newAcc
  }

  override def reset() = acc.clear()

  override def add(path: A) = acc.add(path)

  override def merge(other: AccumulatorV2[A, java.util.Set[A]]) = {
    acc.addAll(other.value)
  }

  override def value: java.util.Set[A] = acc
}
