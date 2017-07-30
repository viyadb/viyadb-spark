package com.github.viyadb.spark.streaming.message

import com.github.viyadb.spark.TableConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

class TsvMessageFactory(table: TableConfig.Table) extends MessageFactory(table) {

  override def createDataFrame(rdd: RDD[AnyRef]): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    spark.read.option("delimiter", "\t").schema(schema).csv(
      spark.createDataset(rdd.map(_.asInstanceOf[String]))(Encoders.STRING))
  }
}