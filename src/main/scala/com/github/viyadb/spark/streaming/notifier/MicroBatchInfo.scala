package com.github.viyadb.spark.streaming.notifier

case class MicroBatchInfo(time: Long, table: String, paths: Iterable[String])
