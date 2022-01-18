package com.proj.spark.exer

import com.proj.spark.utils.FileUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark13_MyPartitioner {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EXer")
    val sc: SparkContext = new SparkContext(config)

    // 针对的是k-v
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("", 10), ("a", 1), ("b", 2), ("c", 3)))

    val partRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))
    FileUtil.deleteFile("output")
    partRDD.saveAsTextFile("output")
  }
}

// 声明分区器
// 继承Partitioner类
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = key match {
    case "" => 1
    case _ => 2
  }
}
