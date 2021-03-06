package com.mylnikov

import com.mylnikov.processor.MessageProcessor
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedOutputStream, BufferedReader, InputStreamReader}

import org.apache.spark.SerializableWritable
import org.json4s.JObject
import org.json4s.jackson.JsonMethods

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

    // Spark init
    val spark = org.apache.spark.sql.SparkSession.builder
  //      .master("local[2]")
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


      val messageProcessor = new MessageProcessor()
      spark.sparkContext.broadcast(messageProcessor)

      val confBroadcast = spark.sparkContext.broadcast(new SerializableWritable(spark.sparkContext.hadoopConfiguration))

      import spark.sqlContext.implicits._
      import org.apache.spark.sql.functions._

      val getText: String => String = JsonMethods.parse(_).asInstanceOf[JObject].values.getOrElse("text", "").toString
      val getTextUdf = udf(getText)
      val hdfsPathInput = conf.hdfsPath()
      val batchResult = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG) as timestamp").
        //extract message properties and timestamp
        withColumn("value", getTextUdf('value)).rdd
        //aggregate each row to count tags
        .map(m => messageProcessor.process(m))
        // reduce result by timestamp and sum tags count
        .reduceByKey((map1, map2) => map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) })

      // Create new file or update exist
      batchResult.foreach(result => {

        // Upload result to hdfs
        val hdfsPath = if(hdfsPathInput.last == '/') hdfsPathInput else hdfsPathInput+"/"
        val fs = FileSystem.get(confBroadcast.value.value)
        val filesIterator = fs.listFiles(new Path(hdfsPath), false)
        var existFiles = mutable.MutableList[String]()
        while (filesIterator.hasNext) {
          existFiles += filesIterator.next().getPath.getName
        }

        if (!existFiles.contains(result._1 + ".txt")) {
          uploadToHDFS(hdfsPath + result._1 + ".txt", fs, result._2)
        } else {
          // File with counts exist so needs to be updated
          val stream = fs.open(new Path(hdfsPath + result._1 + ".txt"))
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