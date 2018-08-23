import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object hello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test2").setMaster("local")
    val sc = new SparkContext(conf)
//    unionTest(sc)
//    groupBykeyTest(sc)
//    classAverScore(sc)
    aggregateByKeyTest(sc)

  }

  def sampleTest(sc: SparkContext): Unit ={
    /**
      * 对RDD进行采样。返回RDD的子集
      */
    val rdd1 = sc.parallelize(Array(1 to 10))
    val rdd2 = rdd1.sample(true,0.2f).collect()
    rdd2.foreach(print)

  }

  def unionTest(sc:SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(1,2,3))
    val rdd2 = sc.parallelize(Array(2,3,4))
    val re = rdd1.union(rdd2).distinct().collect()
    re.map(x=>println(x))

  }
  def groupBykeyTest(sc: SparkContext)={
    val rdd1 = sc.parallelize(Array("java","scala","java","java"))
    val par = rdd1.map(x=>(x,1))

    par.collect().map(println(_))

    val re = par.groupByKey().map(x=>{
      var sum=0
      for (i <- x._2){sum+=i}
      (x._1,sum)
    }).collect()
    re.foreach(println)

  }

  /**
    * 计算班级最高分、最低分、总分、平均分
    * 以元组方式返回
    */
  def classAverScore(sc : SparkContext) : Unit={
    //加载数据
    val scoreRdd = sc.textFile("file:///Users/yexin/Desktop/learnSpace/big5-10spark-day02/score.dat")
    //首先将文本映射为三元组
    val pairRdd = scoreRdd.map(line=>{
      var arr = line.split(",")
      (arr(0), arr(1), arr(2))
    })
    //变换成KV
    val kvRdd = pairRdd.map(t=>(t._1,t))
    //按Key进行分组
    val groupRdd = kvRdd.groupByKey()
    //统计结果
    val result = groupRdd.map(t=>{
      var sum = 0f
      var max = Integer.MIN_VALUE+0.0
      var min = Integer.MAX_VALUE+0.0
      var ave = 0f
      for (e<-t._2){
        if (max<e._3.toFloat)
          max = e._3.toFloat
        if (min>e._3.toFloat)
          min=e._3.toFloat
        sum += e._3.toFloat
      }
      ave = sum/t._2.size
      (t._1,"max:"+max,"min:"+min,"sum:"+sum,"ave:"+ave)
    })
    result.collect().map(x=>println(x+x.getClass.getName))
  }


  /**
    * 按照key聚合
    */

  def aggregateByKeyTest(sc : SparkContext) = {
    ArrayBuffer
    val rdd1 =  sc.parallelize(Array(("1.1",1),("1.1",2),("1.1",3),("1.2",6),("1.2",7),("1.2",8)));
    rdd1.aggregateByKey(ArrayBuffer[String]())(//一开始指定一个初始类型，这个类型就是最后需要返回的U，也就是将V => U,这个U必须是可以迭代的类型，不然第二个函数不会执行
      (a:ArrayBuffer[String],b:Int)=>{a += ("00"+b)},//第一个函数，将V转化为U 即，seqOp: (U, V) => U,
      (c:ArrayBuffer[String],d:ArrayBuffer[String])=>{c++=d}//第二个函数，聚合U 即，combOp: (U, U) => U
    ).collect().map(println(_))

  }

  /**
    * 按照KEY排序
    */






}
