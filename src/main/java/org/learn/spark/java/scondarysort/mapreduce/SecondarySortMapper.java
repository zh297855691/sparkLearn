package org.learn.spark.java.scondarysort.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<IntWritable,Text,DateTemperaturePair,Text> {
    DateTemperaturePair pair = new DateTemperaturePair();
    Text value = new Text();
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");
        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);

        pair.setYearMonth(new Text(yearMonth));
        pair.setDay(new Text(day));
        pair.setTemperature(new IntWritable(temperature));
        value.set(tokens[3]);

        context.write(pair,value);

    }
}
