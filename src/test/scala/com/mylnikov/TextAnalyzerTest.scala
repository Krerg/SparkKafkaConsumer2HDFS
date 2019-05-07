package com.mylnikov

import org.scalatest.FunSuite

class TextAnalyzerTest extends FunSuite {

  val searchWords = Array("big data", "ai", "machine learning", "course")

  test("Should proper extract words") {
    val result = TextAnalyzer.getWords("Some big data big data text with machine learning", searchWords)
    assert(result.length == 3)
    assert(result.contains("big data"))
    assert(result.contains("machine learning"))
  }

  test("Should return empty list in case mismatch") {
    val result = TextAnalyzer.getWords("Some text", searchWords)
    assert(result.isEmpty)
  }

}
