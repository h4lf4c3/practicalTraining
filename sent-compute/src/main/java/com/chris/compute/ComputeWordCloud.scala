package com.chris.compute

import com.chris.common.{Config, IK}
import com.chris.common.Config
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 统计词云图
  * 连接kafka读取评价数据
  *
  * create 'comment','info'
  *
  */
object ComputeWordCloud {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("data/checkpoint")


    val topics = Map("com" -> 2)

    val params = Map(
      "zookeeper.connect" -> Config.getString("kafka.zookeeper.connect"),
      "group.id" -> "asdasfasd",//每一次运行的时候需要修改一下
      "auto.offset.reset" -> "smallest",
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    //连接kafka,创建DStream
    val kafkaDS: DStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      topics,
      StorageLevel.MEMORY_AND_DISK_SER
    )

    //取出value
    val valueDS = kafkaDS.map(_._2)

    var filterDS = valueDS
      .filter(line => {
        // 过滤脏数据
        line.split("\\|").length == 6
      })

    //对评价进行分词
    val wordsDS = filterDS.flatMap(line => {
      val comment = line.split("\\|")(5)
      val sent_id = line.split("\\|")(1)
      //分词
      val words = IK.fit(comment)
      words.map(word => sent_id + "_" + word)
    })

    //统计词语的数量
    val kvDS = wordsDS.map(word => (word, 1))
    //全局统计
    val countDS = kvDS.updateStateByKey((seq: Seq[Int], option: Option[Int]) => Option(seq.sum + option.getOrElse(0)))

    countDS.print()

    countDS.foreachRDD(rdd => {

      //将结果保存到redis
      rdd.foreachPartition(iter => {
        val jedis = new Jedis(Config.getString("redis.host"))
        jedis.auth("123456")
        iter.foreach(kv => {
          val sent_id = kv._1.split("_")(0)
          val word = kv._1.split("_")(1)
          val count = kv._2

          val key = "word_cloud:" + sent_id

          jedis.hset(key, word, count.toString)
        })

        jedis.close()
      })
    })

    ///启动sparkstreaming

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
