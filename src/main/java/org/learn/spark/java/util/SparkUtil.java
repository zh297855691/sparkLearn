package org.learn.spark.java.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 工具类，实现多种方式创建spark上下文环境sparkContext
 * @author zm
 */

public class SparkUtil {

    /**
     * 该方法使用系统配置（system properties ）创建sparkContext
     * 比如使用 ./bin/spark-submit 命令启动spark时，
     * @return JavaSparkContext
     * @throws Exception
     */
    public static JavaSparkContext createJavaSparkContext() throws Exception{
        return new JavaSparkContext();
    }

    /**
     * @desc 通过指定的 Spark's master URL来创建一个JavaSparkContext
     * @param sparkMasterURL Spark master URL as "spark://<spark-master-host-name>:7077"
     * @param applicationName application name
     */
    public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String applicationName) throws  Exception {
        JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL,applicationName);
        return ctx;
    }

    public static JavaSparkContext createJavaSparkContext(String applicationName) throws Exception {
        SparkConf conf = new SparkConf().setAppName(applicationName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        return sparkContext;
    }

    public static String version(){
        return "2.10.2";
    }

}
