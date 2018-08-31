package org.learn.spark.java.scondarysort.mapreduce;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器
 * 在一个Reducer中，使得拥有相同键的pair聚合到一个集合中
 * （K,V1）,(K,V2)...(K,Vn) => {K,(V1,V2,...Vn)}
 */
public class DateTemperatureGroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair1 = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;
        return ((DateTemperaturePair) a).getYearMonth().compareTo(((DateTemperaturePair) b).getYearMonth());
    }
}
