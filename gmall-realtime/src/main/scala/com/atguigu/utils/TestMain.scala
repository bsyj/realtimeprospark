package com.atguigu.utils

import org.apache.spark.{SparkConf, SparkContext}

object TestMain {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setAppName("TestMain").setMaster("local[*]"))

  }

}
