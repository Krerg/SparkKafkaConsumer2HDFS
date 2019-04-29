package com.mylnikov

import com.mylnikov.processor.MessageProcessor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{DataTypes, StructType}
import java.io.{BufferedOutputStream, BufferedReader, InputStreamReader}

import scala.collection.mutable

object SparkKafkaConsumer {

  /**
    * Entry point.
    *
    * @param args kafka topic from, kafka bootstrap server, hdfs path, starting and ending offsets (optionally)
    */
  def main(args: Array[String]): Unit = {

    val conf = new Configuration(args)
    conf.verify()

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
        .master("local[2]")
      .appName("SparkKafkaConsumer")
      .getOrCreate

    var offsets = conf.startingOffsets().split(",").map(offset => offset.toInt)

    while(true) {
      val df = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", conf.bootstrapServer())
        .option("subscribe", conf.kafkaTopic())
        .option("startingOffsets", buildJSONOffset(offsets(0), offsets(1), offsets(2), offsets(3), conf.kafkaTopic()))
        .option("endingOffsets", buildJSONOffset(offsets(0)+conf.batchSize().toInt,
          offsets(1)+conf.batchSize().toInt,
          offsets(2)+conf.batchSize().toInt,
          offsets(3)+conf.batchSize().toInt,
          conf.kafkaTopic()))
        .load()

      // Message struct
      val struct = new StructType()
        .add("text", DataTypes.StringType)

      val messageProcessor = new MessageProcessor()
      spark.sparkContext.broadcast(messageProcessor)

      import spark.sqlContext.implicits._
      import org.apache.spark.sql.functions._

      val batchResult = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG) as timestamp").
        //extract message properties and timestamp
        select(from_json('value, struct).as("json"), 'timestamp).select(col("json.*"), col("timestamp")).rdd
        //aggregate each row to count tags
        .map(m => messageProcessor.process(m))
        // reduce result by timestamp and sum tags count
        .reduceByKey((map1, map2) => map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) })


      // Upload result to hdfs
      val hdfsPath = if(conf.hdfsPath().last == '/') conf.hdfsPath() else conf.hdfsPath()+"/"
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val filesIterator = fs.listFiles(new Path(hdfsPath), false)
      var existFiles = mutable.MutableList[String]()
      while (filesIterator.hasNext) {
        existFiles += filesIterator.next().getPath.getName
      }

      // Create new file or update exist
      batchResult.foreach(result => {
        if (!existFiles.contains(result._1 + ".txt")) {
          uploadToHDFS(hdfsPath + result._1 + ".txt", fs, result._2)
        } else {
          // File with counts exist so needs to be updated
          val stream = fs.open(new Path("/" + result._1 + ".txt"))
          val bufferedReader = new BufferedReader(new InputStreamReader(stream))
          val values = bufferedReader.readLine()
          values.split("\\|").foreach(tag => {
            val Array(tagValue, countValue) = tag.split(":")
            // Sum counts
            result._2 += (tagValue -> (result._2.getOrElse(tagValue, 0) + countValue.toInt))
          })
          uploadToHDFS(hdfsPath + result._1 + ".txt", fs, result._2)
        }
      })
      offsets = offsets.map(offset => offset+conf.batchSize().toInt)
    }


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

  def buildJSONOffset(offset1: Int, offset2: Int, offset3: Int, offset4: Int,topic: String) : String = {
    f"""{"$topic":{"0":$offset1, "1":$offset2, "2":$offset3, "3":$offset4}}"""
  }

}