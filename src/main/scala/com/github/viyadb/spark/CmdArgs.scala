package com.github.viyadb.spark

import scopt._

case class CmdArgs(consulHost: String = "localhost", consulPort: Int = 8500,
                   consulToken: Option[String] = None, consulPrefix: String = "viyadb-cluster",
                   table: String = "")

object CmdArgs {
  def parse(args: Array[String]) = {
    new OptionParser[CmdArgs]("spark-submit") {

      opt[String]("consul-host").optional().action((x, c) =>
        c.copy(consulHost = x)).text("Consul URL (default: localhost)")

      opt[Int]("consul-port").optional().action((x, c) =>
        c.copy(consulPort = x)).text("Consul port number (default: 8500)")

      opt[String]("consul-token").optional().action((x, c) =>
        c.copy(consulToken = Some(x))).text("Consul token if required")

      opt[String]("consul-prefix").optional().action((x, c) =>
        c.copy(consulToken = Some(x))).text("Consul key-value prefix path (default: viyadb-cluster)")

      opt[String]("table").action((x, c) =>
        c.copy(table = x)).text("Name of the table to process")

      help("help").text("prints this usage text")

    }.parse(args, CmdArgs()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Wrong usage")
    }
  }
}