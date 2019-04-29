package com.mylnikov

import scala.collection.mutable

object TextAnalyzer {

  def getWords(text: String, wordsToSearch: Array[String]): mutable.MutableList[String] = {
    val findWords: mutable.MutableList[String] = mutable.MutableList[String]()
    val textWords = text.split(" ")
    import scala.util.control.Breaks._
    for (words <- wordsToSearch) {
      val splittedWords = words.split(" ")
      for (word <- splittedWords) {

      }
    }
    findWords
  }


}
