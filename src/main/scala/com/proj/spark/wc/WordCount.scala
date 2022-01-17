package com.proj.spark.wc

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * bin/spark-submit \
 * --class com.proj.spark.WordCount \
 * WordCount-jar-with-dependencies.jar
 */
object WordCount {
  def main(args: Array[String]): Unit = {

//    delFile("output")
//    return

    // 1.创建SparkConf并设置App名称
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 创建spark上下文对象
    val sc: SparkContext = new SparkContext(config)

    // 读取文件，将文件内容一行一行地读取出来
    // 路径查找位置默认从当前的部署环境中查找
    // 如果需要从本地查找：file:///opt/module/spark/in
        val lines: RDD[String] = sc.textFile("file:///opt/module/spark/in")
//    val lines: RDD[String] = sc.textFile("in/txt",5)

    // 将一行一行的数据分解一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 为了统计方便，将单词数据进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 对转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)


    // hdfs://linux102:9000/user/hadoop/output already exists
    wordToSum.sortBy(x => x._2, ascending = false).saveAsTextFile("output")

    // 将统计结果采集后打印到控制台
    println(wordToSum.collect().mkString(","))

    // 关闭连接
        sc.stop()
  }

  def delFile(path: String): Unit = {
    val file = new File(path)
    if (file.isDirectory) {
      file.listFiles().foreach(x => {
        println(x.getPath)
        delFile(x.getPath)
      })
    }
    file.delete()
  }
}
