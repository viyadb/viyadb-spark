package com.github.viyadb.spark.util

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * Writes pair RDD two different directories prefixes denoted by keys
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    s"${key.asInstanceOf[String]}/${name}"

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    try {
      super.checkOutputSpecs(ignored, job)
    } catch {
      // We swallow this exception here to allow writing under the same path with
      // different key that will constitute the suffix
      case _: FileAlreadyExistsException =>
    }
  }
}