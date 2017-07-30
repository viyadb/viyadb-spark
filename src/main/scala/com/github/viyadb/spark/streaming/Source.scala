package com.github.viyadb.spark.streaming

import org.apache.spark.streaming.StreamingContext

abstract class Source {

  def start(ssc: StreamingContext)
}
