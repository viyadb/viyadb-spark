package com.github.viyadb.spark.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

object TimeUtil {

  abstract class TimeFormat extends Serializable {
    def parse(str: String): Timestamp
    def format(timestamp: Timestamp): String
  }

  class StrTimeFormat(javaFormat: String) extends TimeFormat {
    val format = new SimpleDateFormat(javaFormat)

    override def parse(str: String): Timestamp = {
      new Timestamp(format.parse(str).getTime)
    }

    override def format(timestamp: Timestamp): String = {
      format.format(timestamp)
    }
  }

  class PosixTimeFormat() extends TimeFormat {
    override def parse(str: String): Timestamp = {
      new Timestamp(str.toLong * 1000L)
    }

    override def format(timestamp: Timestamp): String = {
      (timestamp.getTime / 1000L).toString
    }
  }

  class MillisTimeFormat() extends TimeFormat {
    override def parse(str: String): Timestamp = {
      new Timestamp(str.toLong)
    }

    override def format(timestamp: Timestamp): String = {
      timestamp.getTime.toString
    }
  }

  class MicrosTimeFormat() extends TimeFormat {
    override def parse(str: String): Timestamp = {
      val v = str.toLong
      val ts = new Timestamp(v / 1000L)
      ts.setNanos((v % 1000000L).toInt * 1000)
      ts
    }

    override def format(timestamp: Timestamp): String = {
      val v = timestamp.getTime * 1000L
      (v - v % 1000000L + timestamp.getNanos / 1000L).toString
    }
  }

  private val strptime2JavaFormat = Map(
    'a' -> "EEE",
    'A' -> "EEEE",
    'b' -> "MMM",
    'B' -> "MMMM",
    'c' -> "EEE MMM dd HH:mm:ss yyyy",
    'd' -> "dd",
    'H' -> "HH",
    'I' -> "hh",
    'j' -> "DDD",
    'm' -> "MM",
    'M' -> "mm",
    'p' -> "a",
    'S' -> "ss",
    'U' -> "ww",
    'W' -> "ww",
    'x' -> "MM/dd/yy",
    'X' -> "HH:mm:ss",
    'y' -> "yy",
    'Y' -> "yyyy",
    'Z' -> "zzz"
  )

  private val notSupported = Set('w', 'f')

  def convertStrptimeFormat(format: String): String = {
    val builder = new StringBuilder()
    var directive = false
    var inQuote = false

    format.foreach { char =>
      if (char == '%' && !directive) {
        directive = true
      } else {
        if (!directive) {
          // ascii letters are considered SimpleDateFormat directive patterns unless escaped
          val needsQuote = (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z')
          if (needsQuote && !inQuote || !needsQuote && inQuote) {
            builder.append("'")
            inQuote = needsQuote
          }
          if (char == '\'') {
            // a single quote always needs to be escaped, regardless whether already in a quote or not
            builder.append("'")
          }
          builder.append(char)
        } else {
          if (inQuote) {
            builder.append("'")
            inQuote = false
          }

          val translated = strptime2JavaFormat.get(char)
          if (translated.isEmpty && notSupported.contains(char)) {
            throw new IllegalArgumentException(s"Can't convert strptime format to Joda style: ${format}")
          } else {
            builder.append(translated.getOrElse(char))
            directive = false
          }
        }
      }
    }
    if (inQuote) {
      builder.append("'")
    }
    builder.toString()
  }

  def strptime2JavaFormat(format: String): TimeFormat = {
    format match {
      case "posix" => new PosixTimeFormat
      case "millis" => new MillisTimeFormat
      case "micros" => new MicrosTimeFormat
      case other => new StrTimeFormat(convertStrptimeFormat(other))
    }
  }
}
