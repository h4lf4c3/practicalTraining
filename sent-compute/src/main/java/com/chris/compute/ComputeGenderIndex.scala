package com.chris.compute

import com.chris.common.Config
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, HConnectionManager}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis

object ComputeGenderIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("app")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    import sQLContext.implicits._

    //创建spark streaming上下文对象
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint("data/checkpoint")

    val params = Map(
      "zookeeper.connect" -> Config.getString("kafka.zookeeper.connect"),
      "group.id" -> "a",
      "auto.offset.reset" -> "smallest",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val topics = Map("com" -> 4)

    //读取kafka数据   评价表数据
    val commentDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, params, topics, StorageLevel.MEMORY_AND_DISK_SER
    )

    /**
      * 关联用户表
      *
      */

    val filterDS = commentDS
      .map(_._2)
      .filter(_.split("\\|").length == 6)


    //关联hbase微博用户表，取出性别
    val kvDS = filterDS.mapPartitions(iter => {
      //创建hbase连接
      val conf: Configuration = new Configuration
      conf.set("hbase.zookeeper.quorum", Config.getString("hbase.zookeeper.quorum"))
      val connection = HConnectionManager.createConnection(conf)
      //create 'comment_sentiment' ,{NAME => 'info' ,VERSIONS => 100}
      val user = connection.getTable("user")

      iter.map(line => {
        val split = line.split("\\|")
        val sent_id = split(1)
        val user_id = split(4)

        val get = new Get(user_id.getBytes())
        //指定需要查询的列
        get.addColumn("info".getBytes(), "gender".getBytes())
        val result = user.get(get)
        val gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()))

        (sent_id + "_" + gender, 1)
      })
    })


    //统计性别人数
    val countDS = kvDS.updateStateByKey((seq: Seq[Int], option: Option[Int]) => Option(seq.sum + option.getOrElse(0)))

    //将结果保存到redis
    countDS.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //创建redis连接
        val jedis = new Jedis(Config.getString("redis.host"))

        iter.foreach(kv => {
          val sentId = kv._1.split("_")(0)
          val gender = kv._1.split("_")(1)

          val key = "gender:" + sentId

          jedis.hset(key, gender, kv._2.toString)
        })

        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
