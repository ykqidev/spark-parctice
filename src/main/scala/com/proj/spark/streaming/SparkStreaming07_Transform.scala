package com.proj.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {
    // 使用SparkStreaming完成WordCount
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期，以指定的师姐为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux102", 9999)

    // TODO 代码（Driver）(1)
//    socketLineDStream.map{
//      case x => {
//        // TODO 代码（Executor）(n)
//        x
//      }
//    }

    // TODO 代码（Driver）(1)
//    socketLineDStream.transform{
//      case rdd => {
//        // TODO 代码（Driver）(m=采集周期)
//        rdd.map{
//          case x => {
//            // TODO 代码（Executor）(n)
//            x
//          }
//        }
//      }
//    }


    // 一个DStream里面可以有一个或多个RDD
//    socketLineDStream.foreachRDD()



    // 启动采集器
    streamingContext.start()
    // Drvier等待采集器的执行
    streamingContext.awaitTermination()

  }

}
