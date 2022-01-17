package com.proj.spark.utils

import java.io.File


object FileUtil {

  def getFileName(path: String): Array[String] = {
    val file = new File(path)
    file.list
  }

  var fileNameList: List[String] = _

  def getAllFileName(path: String): List[String] = {
    val file = new File(path)
    val files: Array[File] = file.listFiles
    if (fileNameList == null) fileNameList = file.list.toList
    else
      fileNameList = List.concat(fileNameList, file.list.toList)
    if (files != null) for (f <- files) {
      if (f.isDirectory) getAllFileName(f.getAbsolutePath)
    }
    fileNameList
  }

  /**
   * 获取一个文件或目录的大小
   */
  def getLength(file: File): Long = {
    var length: Long = 0
    if (file.isFile) length = file.length
    else {
      val listFiles: Array[File] = file.listFiles
      if (listFiles != null) for (listFile <- listFiles) {
        length += getLength(listFile)
      }
    }
    length
  }

  def deleteFile(filePath: String): Boolean = {
    deleteFile(new File(filePath))
  }

  def deleteFile(file: File): Boolean = {
    if (file.isDirectory) {
      val listFiles: Array[File] = file.listFiles
      if (listFiles != null) for (listFile <- listFiles) {
        deleteFile(listFile)
      }
    }
    file.delete
  }

  def main(args: Array[String]): Unit = {

//    println(getAllFileName("E:\\Idea_Projects\\spark-practice\\src\\main\\scala\\com\\proj\\spark"))

    println(deleteFile("output"))
  }
}
