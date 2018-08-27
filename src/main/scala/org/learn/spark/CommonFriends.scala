package org.learn.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用spark进行共同好友的额判断
  */
object CommonFriends {
  def main(args: Array[String]): Unit = {
    //1、初始化spark参数
    val conf = new SparkConf().setAppName("CommonFriends").setMaster("local")
    val sc = new SparkContext(conf)

    //2、加载数据
    val data = sc.textFile("/Users/yexin/Desktop/learnSpace/sparkLearn/conf/data.txt")

    //3、映射为(K,V)对
    val rdd1 = data.flatMap(x=>{
      val person = x.split(":")(0)
      for (i<-x.split(":")(1).split(",")) yield (i,person)
    })

    //4、规约
    val result = rdd1.reduceByKey(_+"::"+_).collect().foreach(println)

  }

}
