package com.github.viyadb.spark.util

import java.io.IOException

import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats}

import scalaj.http._

class ConsulClient(hostname: String = "localhost", port: Int = 8500, token: Option[String] = None) extends Serializable {

  class ConsulException(msg: String) extends IOException(msg)

  protected def get(path: String): String = {
    val r = Http(s"http://${hostname}:${port}/${path.stripPrefix("/")}")
      .option(HttpOptions.connTimeout(3000)).asString
    if (!r.is2xx) {
      throw new ConsulException("Wrong HTTP status returned")
    }
    r.body
  }

  def kvPut(path: String, data: String): Unit = {
    if (!Http(s"http://$hostname:$port/v1/kv/${path.stripPrefix("/")}")
      .put(data)
      .option(HttpOptions.connTimeout(3000))
      .header("content-type", "application/json").asString.is2xx) {
      throw new ConsulException("Wrong HTTP status returned")
    }
  }

  def kvGet(path: String): String = {
    get(s"v1/kv/${path.stripPrefix("/")}?raw")
  }

  def kvList(path: String): List[String] = {
    implicit val formats: Formats = DefaultFormats
    JsonMethods.parse(get(s"v1/kv/${path.stripPrefix("/")}?keys")).extract[List[String]]
  }
}
