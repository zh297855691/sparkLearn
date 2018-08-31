package org.learn.spark.java.scondarysort.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 定制分区器
 * @desc 根据mapper的键，决定一个 <Key,Value>的目标Reducer
 *       一般来说，一个MR程序只有一个reducer，但是某些情况下，当存在多个reducer时，需要指定分区方法
 *       默认情况下，Hadoop会使用KEY的hashcode作为分区编码，但在二次排序中，key实际上是一个组合键，
 *       如果不指定分区方法，会使得相同自然键（年月）可能会发到不同的reducer中，由于一个reducer会产生一个输出文件，
 *       因此达不到二次排序的目的，（因为相同自然键的pair会输出到多个reducer）
 * @author zm
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair,Text>{
    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numOfPartition) {
        //必须确保分区数是非负的,并且保证拥有相同自然键的pair进入同一个reducer
        return Math.abs(pair.getYearMonth().hashCode()%numOfPartition);
    }
}
