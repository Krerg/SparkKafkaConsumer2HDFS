package com.mylnikov

import org.rogach.scallop._

class Configuration(arguments: Seq[String]) extends ScallopConf(arguments) {

  val kafkaTopic = opt[String] (required = true)
  val bootstrapServer = opt[String] (required = true)
  val hdfsPath = opt[String] (required = true)
  val startingOffsets = opt[String] (required = true)
  val batchSize = opt[String] (default = Option("10"), required = true)

}
