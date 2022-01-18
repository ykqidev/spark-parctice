package com.proj.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {

    //    val ints = List(1, 2, 3, 4, 5, 6, 7)
    //
    //    // 滑动窗口函数
    //    val iterator: Iterator[List[Int]] = ints.sliding(3,3)
    //
    //    for (elem <- iterator) {
    //      println(elem.mkString(","))
    //    }

    // 使用SparkStreaming完成WordCount
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期，以指定的师姐为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 保存数据的状态，需要设定检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    // 指定文件夹中采集数据
    //    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("linux102", 9999))
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "linux102:2181",
      "hadoop",
      Map("hadoop" -> 3)
    )

    // 窗口大小应该为采集周期的整数倍，窗口滑动的步长也应该为采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    // 将采集的数据进行分解（扁平化）
    //    val wordDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))
    val wordDStream: DStream[String] = windowDStream.flatMap(t => t._2.split(" "))

    // 将数据进行结构的装换方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordToSumDStream.print()

    // 启动采集器
    streamingContext.start()
    // Drvier等待采集器的执行
    streamingContext.awaitTermination()

  }

}
