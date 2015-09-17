package com.test

import org.apache.spark.{SparkConf, SparkContext}

object SimpleAppRdd {
  def main(args: Array[String]) {
    val logFile = "/Users/gyqgd/Projects/spark/simple/src/main/scala/com/test/SimpleApp.scala" // 应该是你系统上的某些文件
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
