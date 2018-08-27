package org.learn.spark.org.learn.spark.commonFriend

import org.apache.spark.{SparkConf, SparkContext}

object CommonFriendsStepOne {
  def main(args: Array[String]): Unit = {
    //加载数据
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("CommonFriendsStepOne")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///Users/yexin/Desktop/workSpace/dc/VUE/VI/vue_exercise/sparkLearn/conf/data.txt")

    //step1: A:B,C,D,F,E,O       =>        B--> A   C-->A  D-->A  F-->A ....
    val frdd1 = data.map(x=>(x.split(":")(0),x.split(":")(1).split(",")))
    val rdd1 = data.flatMap(x=>{
      val key = x.split(":")(0)
      val value = x.split(":")(1)
      val tt = value.split(",")
      val mm = tt.map(x=>(x,key))
      mm
    })

    //step2:

    val groupRdd = rdd1.groupByKey().filter(x=>{
      x._2.size>=2
    })


    def PairOfRdd(cb:String)={
      var re = new StringBuilder()
      val list=cb.split(",")
      for (i <- 0 to list.length-1 ){
        for (j <- i+1 to list.length-1)
          println(list(i)+","+list(j))
      }
    }


    val rdd2 = groupRdd.flatMap(x=>{

      val person = x._1
      val sb = new StringBuilder()

      for (i<-x._2){
        sb.append(i)
      }
      sb
    }).map(x=>println(x))


//
    groupRdd.collect().foreach(println)



  }

}
