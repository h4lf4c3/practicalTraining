package com.chris.common

import java.io.StringReader

import org.wltea.analyzer.core.IKSegmenter

import scala.collection.mutable.ListBuffer

object IK {
  def main(args: Array[String]): Unit = {

    println(IK.fit("周扬青指定还有瓜 不然罗志祥凌晨五点又道歉"))

  }

  /**
    * ik分词器
    *
    * @return
    */
  def fit(text: String): List[String] = {
    val sr = new StringReader(text)
    val ik = new IKSegmenter(sr, true)

    val lf = new ListBuffer[String]

    var lex = ik.next
    while (lex != null) {
      lf.+=(lex.getLexemeText)
      lex = ik.next
    }

    lf.toList
  }
}
