package com.proj.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Spark框架之SparkStreaming消费Kafka数据手动提交offset到zookeeper
 * kafka对接是parkstreaming，手动维护offset到zookeeper
 * kafka 是0.8版本
 */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {

    // 初始化ssc
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")

    // kafka参数
    val brokers = "linux102:9092,linux103:9092,linux104:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization)
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].toString)

    //创建KafkaCluster对象，维护offset
    val cluster = new KafkaCluster(kafkaParams)

    //获取初始偏移量
    val fromOffset: Map[TopicAndPartition, Long] = getOffset(cluster, group, topic)

    // 如果是保存在mysql,要起事务，手动提交
    //创建流
    val kafkaStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams, fromOffset, (mess: MessageAndMetadata[String, String]) => mess.message())

    //转换逻辑
    kafkaStream.map((_, 1)).reduceByKey(_ + _).print()

    //提交offset
    setOffset(cluster, kafkaStream, group)


    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 获取偏移量
   *
   * @param cluster
   * @param group
   * @param topic
   * @return
   */
  def getOffset(cluster: KafkaCluster, group: String, topic: String) = {
    var partitionToLong = new mutable.HashMap[TopicAndPartition, Long]()

    //获取所有主题的分区
    val topicAndPartition: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(Set(topic))

    val partitions: Set[TopicAndPartition] = topicAndPartition.right.get

    //获取偏移量信息
    val offsetInfo: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(group, partitions)

    if (offsetInfo.isRight) {
      // 如果有offset信息则存储offset
      val offsets: Map[TopicAndPartition, Long] = offsetInfo.right.get
      for (offset <- offsets) {
        partitionToLong += offset
      }

    } else {
      //如果没有则设置为0
      for (p <- partitions) {
        partitionToLong += (p -> 0L)
      }
    }

    partitionToLong.toMap
  }


  /**
   * 提交偏移量
   */
  def setOffset(cluster: KafkaCluster, kafkaStream: InputDStream[String], group: String): Unit ={
    kafkaStream.foreachRDD{rdd=>
      val offsetRangeArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offset <- offsetRangeArray){
        val ack : Either[Err,Map[TopicAndPartition,Short]] = cluster.setConsumerOffsets(group,Map(offset.topicAndPartition -> offset.untilOffset))
        if (ack.isRight) {
          println(s"成功更新了消费kafka的偏移量：${offset.untilOffset}")
        } else {
          println(s"失败更新消费kafka的偏移量：${ack.left.get}")
        }
      }
    }
  }

}
