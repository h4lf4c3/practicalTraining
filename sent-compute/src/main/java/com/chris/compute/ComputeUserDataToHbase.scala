package com.chris.compute

import com.chris.common.Config
import com.chris.common.Config
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object ComputeUserDataToHbase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("app")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    //创建spark streaming上下文对象
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("data/checkpoint")

    val params = Map(
      "zookeeper.connect" -> Config.getString("kafka.zookeeper.connect"),
      "group.id" -> "asfasd",
      "auto.offset.reset" -> "smallest",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val topics = Map("user" -> 4)

    //读取kafka数据   评价表数据
    val userDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, params, topics, StorageLevel.MEMORY_AND_DISK_SER
    )

    /**
      * 将用户数据保存到hbase
      *
      * create 'user','info'
      *
      */

    userDS
      .filter(_._2.split("\\|").length == 6)
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          //创建hbase连接
          //创建hbase连接
          val conf: Configuration = new Configuration
          conf.set("hbase.zookeeper.quorum", Config.getString("hbase.zookeeper.quorum"))
          val connection = HConnectionManager.createConnection(conf)
          //create 'comment_sentiment' ,{NAME => 'info' ,VERSIONS => 100}
          val user = connection.getTable("user")

          iter.foreach(line => {
            val split = line._2.split("\\|")
            val id = split(0)
            val user_name = split(1)
            val gender = split(2)
            val description = split(3)
            val follow_count = split(4)
            val followers_count = split(5)

            val put = new Put(id.getBytes)

            put.add("info".getBytes(), "user_name".getBytes(), user_name.getBytes())
            put.add("info".getBytes(), "gender".getBytes(), gender.getBytes())
            put.add("info".getBytes(), "description".getBytes(), description.getBytes())
            put.add("info".getBytes(), "follow_count".getBytes(), follow_count.getBytes())
            put.add("info".getBytes(), "followers_count".getBytes(), followers_count.getBytes())

            user.put(put)
          })

          connection.close()
        })

      })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
