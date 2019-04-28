package com.mylnikov

import com.mylnikov.processor.MessageProcessor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{DataTypes, StructType}
import java.io.BufferedOutputStream

import scala.collection.mutable

object SparkKafkaConsumer {

  /**
    * Entry point.
    *
    * @param args kafka topic from, kafka bootstrap server, hdfs path, starting and ending offsets (optionally)
    */
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      throw new IllegalArgumentException("You should specify kafka topic, kafka broker address, hdfs path")
    }

    var startingOffset = ""
    var endingOffset = ""
    if (args.length == 5) {
      startingOffset = args(3)
      endingOffset = args(4)
    } else {
      startingOffset = "earliest"
      endingOffset = "latest"
    }

    // Spark init
    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("SparkKafkaConsumer")
      .getOrCreate

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", args(1))
      .option("subscribe", args(0))
      .option("startingOffsets", startingOffset)
      .option("endingOffsets", endingOffset)
      .load()

    // Message struct
    val struct = new StructType()
      .add("userName", DataTypes.StringType)
      .add("location", DataTypes.StringType)
      .add("text", DataTypes.StringType)

    val messageProcessor = new MessageProcessor()
    spark.sparkContext.broadcast(messageProcessor)

    val batchResult = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG) as timestamp").
      //extract message properties and timestamp
      select(functions.from_json(functions.col("value"), struct).as("json"), functions.col("timestamp")).select(functions.col("json.*"), functions.col("timestamp")).rdd
      //aggregate each row to count tags
      .map(m => messageProcessor.process(m))
      // reduce result by timestamp and sum tags count
      .reduceByKey((map1, map2) => map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) })
      .collect()

    // Upload result to hdfs
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filesIterator = fs.listFiles(new Path("/"), false)
    var existFiles = mutable.MutableList[String]()
    while (filesIterator.hasNext) {
      existFiles += filesIterator.next().getPath.getName
    }

    // Create new file or update exist
    batchResult.foreach(result => {
      if (!existFiles.contains(result._1 + ".txt")) {
        uploadToHDFS(args(2) + result._1 + ".txt", fs, result._2)
      } else {
        // File with counts exist so needs to be updated
        val stream = fs.open(new Path("/" + result._1 + ".txt"))
        val values = stream.readLine()
        values.split("\\|").foreach(tag => {
          val tagValue = tag.split(":")(0)
          val countValue = tag.split(":")(1)
          // Sum counts
          result._2 += (tagValue -> (result._2.getOrElse(tagValue, 0) + countValue.toInt))
        })
        uploadToHDFS(args(2) + result._1 + ".txt", fs, result._2)
      }
    })
  }

  /**
    * Uploads result map with aggregation to hdfs.
    *
    * @param path   hdfs path
    * @param fs     hdfs file system object
    * @param result map woth aggregated tags
    */
  def uploadToHDFS(path: String, fs: FileSystem, result: mutable.Map[String, Int]): Unit = {
    val output = fs.create(new Path(path))
    val os = new BufferedOutputStream(output)
    os.write(result.map { case (k, v) => k + ":" + v }.mkString("|").getBytes("UTF-8"))
    os.close()
  }

}
