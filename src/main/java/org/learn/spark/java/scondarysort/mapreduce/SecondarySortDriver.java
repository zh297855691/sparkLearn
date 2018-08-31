package org.learn.spark.java.scondarysort.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class SecondarySortDriver extends Configured implements Tool {
    private static final int LENGTH = 2;
    /**
     * 加载log4j日志
     */
    public static Logger logger = Logger.getLogger(SecondarySortDriver.class);
    public static void main(String[] args) throws Exception {
        //确保主函数传入两个参数

        if (args.length != LENGTH){
            logger.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
        }

        int returnStatus = submitJob(args);
        logger.info("return status = " + returnStatus);
        System.exit(returnStatus);

    }

    /**
     * @desc 启动run线程，提交MR程序
     *@param args
     * @return int
     * @throws Exception
     */
    public static int submitJob(String[] args) throws Exception {

        int returnStatus = ToolRunner.run(new SecondarySortDriver(),args);
        return returnStatus;
    }




    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance();
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySortDriver");

        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);
        return status ? 0:1;
    }
}
