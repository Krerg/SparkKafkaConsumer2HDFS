package com.mylnikov.processor

import org.scalatest.FunSuite

class MessageProcessorTest extends FunSuite{

  val messageProcessor = new MessageProcessor

  test("Should aggregate text") {
    val result = messageProcessor.aggregateText("big data text with machine learning and ai", 1556064960L)
    assert(result._1 == "1970-01-18_16-00")

    assert(result._2.keySet.contains("big data"))
    assert(result._2.keySet.contains("machine learning"))
    assert(result._2.keySet.contains("ai"))

    assert(result._2("big data") == 1)
    assert(result._2("machine learning") == 1)
    assert(result._2("ai") == 1)

  }

}
