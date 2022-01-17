package com.proj.spark.exer

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Accumulator {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Exer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[String] = sc.makeRDD(List("hello","hadoop","spark"), 2)

    // val i:Int = dataRDD.reduce(_+_)
    // println(i)
    var sum: Int = 0
    // 使用累加器来共享变量，来累加数据

    // 创建累加器对象
    val wordAccumulator = new WordAccumulator
    // 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach {
      case word => {
        // 执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }

    // 获取累加器的值
    println("sum = " + wordAccumulator.value)

    sc.stop()


  }
}

// 声明累加器
// 1.继承AccumulatorV2
// 2.实现抽象方法
// 3.声明累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }

  // 重置累加器对象
  override def reset(): Unit = list.clear()

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if(v.contains("h")){
      list.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}


