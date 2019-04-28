package com.mylnikov.processor

import java.util.Calendar

import com.mylnikov.TextAnalyzer
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Message aggregator.
  */
class MessageProcessor extends Serializable {

  /**
    * Big data tags
    */
  val searchWords = Array("big data", "ai", "machine learning", "course")

  def process(row: Row): (String, mutable.Map[String, Int]) = {
    aggregateText(row.getAs[String]("text"), row.getAs[Long]("timestamp"))
  }

  /**
    * Aggregates text by extracting big data words from it and count them.
    *
    * @param text input text to aggregate
    * @param timestamp text's timestamp from kafka
    * @return pair of formatted timestamp and map with tags and its count
    */
  def aggregateText(text: String, timestamp: Long): (String, mutable.Map[String, Int]) = {

    val windowStart = Calendar getInstance()
    windowStart.setTimeInMillis(timestamp)
    windowStart.set(Calendar.MINUTE, 0)
    windowStart.set(Calendar.SECOND, 0)

    val aggregatorMap = mutable.Map[String, Int]()

    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm")

    TextAnalyzer.getWords(text, searchWords)
      .foreach(word => {
        aggregatorMap += (word -> (aggregatorMap.getOrElse(word, 0) + 1))
      })

    format.format(windowStart.getTime) -> aggregatorMap
  }

}
