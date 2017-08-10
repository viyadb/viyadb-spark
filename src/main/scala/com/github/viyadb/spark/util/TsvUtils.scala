package com.github.viyadb.spark.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object TsvUtils {

  implicit class TsvRow(row: Row) extends java.io.Serializable {
    def toTsv(): String = {
      row.toSeq.map(_ match {
        case s: String => s.replaceAll("[\t\n\r\u0000\\\\]", "").trim
        case null => ""
        case any => any
      }).mkString("\t")
    }
  }

  implicit class TsvRDD(rdd: RDD[Row]) extends java.io.Serializable {
    def toTsv(): RDD[String] = {
      return rdd.map(_.toTsv)
    }
  }

  implicit class TsvDataFrame(df: DataFrame) extends java.io.Serializable {
    def toTsvRDD(): RDD[String] = {
      df.rdd.toTsv()
    }
  }
}