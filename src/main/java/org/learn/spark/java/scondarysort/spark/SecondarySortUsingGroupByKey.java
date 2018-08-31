package org.learn.spark.java.scondarysort.spark;

/**
 * @desc 使用groupByKey实现内存中的二次排序
 * @Input:
 *
 *    name, time, value
 *    x,2,9
 *    y,2,5
 *    x,1,3
 *    y,1,7
 *    y,3,1
 *    x,3,6
 *    z,1,4
 *    z,2,8
 *    z,3,7
 *    z,4,0
 *
 * @Output: generate a time-series looking like this:
 *
 *       t1 t2 t3 t4
 *  x => [3, 9, 6]
 *  y => [7, 5, 1]
 *  z => [4, 8, 7, 0]
 *
 *  x => [(1,3), (2,9), (3,6)]
 *  y => [(1,7), (2,5), (3,1)]
 *  z => [(1,4), (2,8), (3,7), (4,0)]
 *  @author zm
 */
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.learn.spark.java.util.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SecondarySortUsingGroupByKey {
    public static final int LENGTH = 2;
    public static void main(String[] args) throws Exception {

        workForTemp(args);


    }

    public static void workForNameTimeValue(String[] args){

    }

    public static void workForTemp(String[] args) throws Exception {
        //1、读取输入参数并验证,需指定输入路径与输出路径
        if (args.length <  LENGTH){
            System.err.println("Usage: SecondarySortUsingGroupByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);
        //2、通过sparkContext连接Spark Master
        JavaSparkContext sc = SparkUtil.createJavaSparkContext("local","SecondarySorting");
        //3、使用JavaSparkContext创建JavaRDD
        JavaRDD<String> line = sc.textFile(inputPath, 1);
        //4、将JavaRDD转化为pairRDD  2000,12,04,10 =>(2000-12,10）
        PairFunction<String, String, Integer> pairFunction = new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception{
                String[] token = s.split(",");
                String yearMonth = token[0]+"-"+token[1];
                String day = token[2];
                Integer tem = Integer.valueOf(token[3]);
                return new Tuple2<>(yearMonth,tem);
            }
        };
        //输出验证
        JavaPairRDD<String, Integer> pairRDD = line.mapToPair(pairFunction);
        List<Tuple2<String, Integer>> collect = pairRDD.collect();
        for (Tuple2 i :
                collect) {
            System.out.println(i._1 + "::" + i._2);
        }
        //5、按键分组
        JavaPairRDD<String, Iterable<Integer>> groups = pairRDD.groupByKey();
        //输出验证
        System.out.println("====DEBUG1=====");
        List<Tuple2<String, Iterable<Integer>>> groupsList = groups.collect();
        for (Tuple2<String, Iterable<Integer>> t :
                groupsList) {

            StringBuilder sb = new StringBuilder();
            System.out.println(t._1);
            for (Integer i :
                    t._2) {
                sb.append(i+",");
            }
            System.out.println(sb.toString());
        }
        System.out.println("==========");
        //6、内存中按键排序

        Function<Iterable<Integer>, Iterable<Integer>> vuFunction = new Function<Iterable<Integer>, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Iterable<Integer> v1) throws Exception {
                //RDD具有不可变性，这一点非常重要，无论是对RDD本省还是其中包含的元素
                //spark强大的mapValues可以使得无序的values转化为有序的集合，但是由于RDD的不可变性，必须首先进行复制操作
                List<Integer> list = new ArrayList<>();
                List SortedList = IteratorUtils.toList(v1.iterator());
                Collections.sort(SortedList);
                return SortedList;
            }
        };
        JavaPairRDD<String, Iterable<Integer>> sortedRdd = groups.mapValues(vuFunction);
        List<Tuple2<String, Iterable<Integer>>> collect1 = sortedRdd.collect();
        System.out.println("==========DEBUG2===========");
        for (Tuple2<String, Iterable<Integer>> t :
                collect1) {
           System.out.print(t._1 + "->");
            for (Integer i :
                    t._2) {
                System.out.print(i + "::");
            }
            System.out.println();
        }
        System.out.println("=========================");
    }
}
