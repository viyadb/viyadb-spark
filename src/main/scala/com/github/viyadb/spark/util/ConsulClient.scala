package com.github.viyadb.spark.util

import java.io.IOException

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scalaj.http._

class ConsulClient(hostname: String = "localhost", port: Int = 8500, token: Option[String] = None) extends Serializable {

  class ConsulException(msg: String) extends IOException(msg)

  protected def get(path: String) = {
    val r = Http(s"http://${hostname}:${port}/${path.stripPrefix("/")}").asString
    if (!r.is2xx) {
      throw new ConsulException("Wrong HTTP status returned")
    }
    r.body
  }

  def kvPut(path: String, data: String) = {
    if (!Http(s"http://${hostname}:${port}/${path.stripPrefix("/")}").put(data)
      .header("content-type", "application/json").asString.is2xx) {
      throw new ConsulException("Wrong HTTP status returned")
    }
  }

  def kvGet(path: String): String = {
    get(s"v1/kv/${path.stripPrefix("/")}?raw")
  }

  def kvList(path: String): List[String] = {
    implicit val formats = DefaultFormats
    JsonMethods.parse(get(s"v1/kv/${path.stripPrefix("/")}?keys")).extract[List[String]]
  }
}
