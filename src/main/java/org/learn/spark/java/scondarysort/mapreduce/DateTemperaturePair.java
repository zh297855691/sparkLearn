package org.learn.spark.java.scondarysort.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 该类为存储（K,V）的复合bean类,该类将作为二次排序的组合键
 * 在HADOOP中，如果要持久存储定制数据类，需要实现Writable接口
 * 在HADOOP中，如果要自定义定制数据类的比较方法，需要实现WritableComparable接口，并实现compareTo()方法
 */

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
    //自然主键
    private Text yearMonth = new Text();
    //day
    private Text day = new Text();
    // 温度
    private IntWritable temperature = new IntWritable();

    //基本构造函数
    public DateTemperaturePair() {
    }

    public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature) {
        this.yearMonth = yearMonth;
        this.day = day;
        this.temperature = temperature;
    }

    //重写接口方法
    @Override
    public void write(DataOutput out) throws IOException{
        //将本复合类写入输出流
        this.yearMonth.write(out);
        this.day.write(out);
        this.temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //本复合类从输入流中读取
        this.yearMonth.readFields(in);
        this.day.readFields(in);
        this.temperature.readFields(in);
    }

    //包装类方法，从输入流中读取出一个本复合类
    public DateTemperaturePair read(DataInput in) throws IOException{
        DateTemperaturePair dateTemperaturePair = new DateTemperaturePair();
        dateTemperaturePair.readFields(in);
        return dateTemperaturePair;
    }


    //bean方法
    public Text getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(Text yearMonth) {
        this.yearMonth = yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public void setDay(Text day) {
        this.day = day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(IntWritable temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateTemperaturePair that = (DateTemperaturePair) o;

        //自定义相等判断
        if(this.temperature != null ? !temperature.equals(that.temperature) : that.temperature != null){
            return false;
        }
        if(this.yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {

        return Objects.hash(yearMonth, day, temperature);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append(", day=");
        builder.append(day);
        builder.append(", temperature=");
        builder.append(temperature);
        builder.append("}");
        return builder.toString();
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        /**
         * 该比较强属于本复合类的定制比较器，用于比较两个复合类
         * 在二次排序中，使用组合键作为控制按值排序的手段
         */
        int comparable = this.yearMonth.compareTo(pair.yearMonth);
        if (comparable == 0){
            comparable = this.temperature.compareTo(pair.temperature);
        }
        return 0;
    }
}
