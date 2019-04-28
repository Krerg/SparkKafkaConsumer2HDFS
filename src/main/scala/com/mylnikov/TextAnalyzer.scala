package com.mylnikov

import scala.collection.mutable

object TextAnalyzer {

  def getWords(text: String, wordsToSearch: Array[String]) : mutable.MutableList[String] = {
    val findWords: mutable.MutableList[String] = mutable.MutableList[String]()
    for(word <- wordsToSearch) {
      val indexOfWord = text.indexOf(word)
      if(indexOfWord >= 0) {
        val beforeCharIndex = indexOfWord-1
        val afterCharIndex = indexOfWord+word.length
        if(!isCharIsLetter(text, beforeCharIndex) && !isCharIsLetter(text, afterCharIndex)) {
          findWords+=word
        }
      }
    }
    findWords
  }

  /**
    * @param word input word
    * @param index index of character to check
    * @return true if index of character is a letter, otherwise false
    */
  def isCharIsLetter(word: String, index: Int) : Boolean = {
    if(index < 0) {
      return false
    }
    if(index > (word.length - 1)) {
      return false
    }
    word.charAt(index).isLetter
  }

}
