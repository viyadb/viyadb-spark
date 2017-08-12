package com.github.viyadb.spark.util

import java.io.FileNotFoundException
import java.nio.charset.Charset

import org.apache.hadoop.fs.{FileStatus, Path}

import scala.io.Codec

object FileSystemUtil {

  def delete(path: String): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), new org.apache.hadoop.conf.Configuration())
    try {
      fs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch {
      case _: Throwable => {}
    }
  }

  def exists(path: String): Boolean = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), new org.apache.hadoop.conf.Configuration())
    fs.exists(new org.apache.hadoop.fs.Path(path))
  }

  def setContent(path: String, content: String) = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), new org.apache.hadoop.conf.Configuration())
    val fh = fs.create(new Path(path), true)
    try {
      fh.write(content.getBytes(Charset.forName("UTF-8")))
    } finally {
      fh.close()
    }
  }

  def getContent(path: String): String = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), new org.apache.hadoop.conf.Configuration())
    val fh = fs.open(new Path(path))
    try {
      scala.io.Source.fromInputStream(fh)(Codec.UTF8).mkString
    } finally {
      fs.close()
    }
  }

  def list(path: String): Seq[FileStatus] = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), new org.apache.hadoop.conf.Configuration())
    try {
      fs.listStatus(new Path(path))
    } catch {
      case _: FileNotFoundException => Seq()
    }
  }
}