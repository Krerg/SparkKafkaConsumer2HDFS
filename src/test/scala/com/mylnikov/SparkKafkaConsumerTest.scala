package com.mylnikov

import java.io.{BufferedOutputStream, BufferedReader, InputStreamReader}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSuite

import scala.collection.mutable

class SparkKafkaConsumerTest extends FunSuite with SharedSparkContext {

  test("HDFS test") {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val map: mutable.Map[String, Int] = mutable.Map("timestamp1" -> 6, "timestamp2" -> 3)
    SparkKafkaConsumer.uploadToHDFS("/out.txt", fs, map)
    val stream = fs.open(new Path("/out.txt"))
    val bufferedReader = new BufferedReader(new InputStreamReader(stream))
    val values = bufferedReader.readLine()
    assert(values == "timestamp1:6|timestamp2:3")
    fs.delete(new Path("/out.txt"), true)
  }


}
