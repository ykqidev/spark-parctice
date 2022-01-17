package com.proj.spark.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_GroupBy {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    // 生成数据，按照指定的规则进行分组
    // 分组后的数据形成了对偶组（K-V）,K表示分组的key,v表示分组的数据集合
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

//    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)
    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_ % 2)

    groupRDD.collect().foreach(array => {
      println(array._1 + " " + array._2.mkString(","))
    })
  }
}
