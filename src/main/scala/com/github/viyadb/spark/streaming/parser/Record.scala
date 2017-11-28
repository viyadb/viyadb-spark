package com.github.viyadb.spark.streaming.parser

import org.apache.spark.sql.Row

/**
  * Record represented as Spark's Row
  *
  * @param values Parsed record field values
  */
class Record(val values: Array[Any]) extends Row {

  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.clone()

  override def copy(): Record = this
}
