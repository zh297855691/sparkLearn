package org.learn.spark.java.scondarysort.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<DateTemperaturePair,Text,Text,Text> {
    Text value = new Text();
    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text t :
                values) {
            sb.append(t.toString());
            sb.append(",");
        }
        value.set(sb.toString());
        context.write(key.getYearMonth(),value);
    }
}
