package com.chris.test

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestSpark").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //实时计算
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("data/checkpoint")

    //读取socket测试
    //在node1中执行  nc -lk 5656
    val ds = ssc.socketTextStream("master", 5656)


    ds.flatMap(line => line.split(","))
      .map(word => (word, 1))

      /**
        * updateStateByKey 有状态算子
        *
        * Option:状态，之前的计算结果
        * seq; 当前batch的数据
        */
      .updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      //用当前batch的数据去更新之前的计算结果
      Some(seq.sum + option.getOrElse(0))
    })
      .print()


    //启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
