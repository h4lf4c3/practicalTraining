package com.chris.compute

import java.text.SimpleDateFormat
import java.util.Date

import com.chris.common.{Config, IK}
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDFModel, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ComputeSentimentIndex {
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


    //情感打标
    var flagDS = commentDS
      .map(_._2)
      .filter(_.split("\\|").length == 6)
      .transform(rdd => {
        //分词
        val wordDF = rdd.map(comment => {
          val split = comment.split("\\|")

          val id = split(0)
          val sent_id = split(1)
          val created_at = split(2)

          val format = new SimpleDateFormat("yyyyMMddhh")
          val date = format.format(new Date(created_at))

          val like_count = split(3)
          val user_id = split(4)

          //分词
          val comment_text = IK.fit(split(5)).mkString(" ")

          Comment(id, sent_id, date, like_count, user_id, comment_text)
        }).toDF()


        //Tokenizer  英文分词器
        val tok = new Tokenizer()
          .setInputCol("comment_text")
          .setOutputCol("feature")

        val tokDF = tok.transform(wordDF)


        //计算tf
        val tfModel = new HashingTF()
          //.setNumFeatures(262144) //设置特征数量,  值越大准确率越高   计算复杂度越高
          .setInputCol("feature")
          .setOutputCol("tf")

        val tfDF = tfModel.transform(tokDF)


        //计算idf
        //加载idf模型
        val idfModel = IDFModel.load("model/idfModel")

        val idfDF = idfModel.transform(tfDF)


        //情感打标
        //将数据带入贝叶斯模型

        //加载模型
        val nbModel = NaiveBayesModel.load("model/nbModel")

        //对数据进行打标
        val rsultDF = nbModel.transform(idfDF)
        rsultDF.rdd
      })


    //统计舆情
    val resultDS = flagDS.map(row => {
      val sentimentId = row.getAs[String]("sent_id")
      //预测结果
      var prediction = row.getAs[Double]("prediction")
      //评价时间
      var date = row.getAs[String]("date")
      //正负标记的概率
      val probability = row.getAs[Vector]("probability")




      //计算两个概率的差值
      val p = math.abs(probability(0) - probability(1))
      println(prediction+"\t"+p)
      //如果差值小于0.3评论为中性
      if (p < 0.3) {
        prediction = 2.0
      }
      val key = sentimentId + "_" + date + "_" + prediction

      (key, 1)
    })
      //统计舆情结果
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
      .map(t => {
        val split = t._1.split("_")
        val sentimentId = split(0)
        val date = split(1)
        val flag = split(2)
        val count = t._2

        val key = sentimentId + "_" + date
        val value = flag + ":" + count

        (key, value)
      })
      //将结果转换格式
      /**
        * (1_2019-07-15 11,0.0:10|1.0:6)
        * (0_2019-07-20 18,0.0:3|1.0:2)
        * (1_2019-07-19 22,0.0:6|1.0:4)
        */
      .reduceByKey(_ + "|" + _)


    //将结果写入redis
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {

        //创建hbase连接
        val conf: Configuration = new Configuration
        conf.set("hbase.zookeeper.quorum", Config.getString("hbase.zookeeper.quorum"))
        val connection = HConnectionManager.createConnection(conf)
        //create 'comment_sentiment' ,{NAME => 'info' ,VERSIONS => 100}
        val sentiment = connection.getTable("comment_sentiment")

        iter.foreach(t => {
          val split = t._1.split("_")
          val sentimentId = split(0)
          //以时间作为版本号
          val time = split(1)
          val format = new SimpleDateFormat("yyyyMMddhh")
          val ts = format.parse(time).getTime

          val put = new Put(sentimentId.getBytes())

          put.add("info".getBytes(), "real".getBytes(), ts, t._2.getBytes())

          sentiment.put(put)
        })

        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  case class Comment(id: String, sent_id: String, date: String, like_count: String, user_id: String, comment_text: String)

}
