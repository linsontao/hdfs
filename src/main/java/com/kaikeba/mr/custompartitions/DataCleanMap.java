package com.kaikeba.mr.custompartitions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataCleanMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        //自定义counter用于统计异常的词频
        Counter counter = context.getCounter("DataClean", "errorwordcount");
        //样例数据：20111230111308	0bf5778fc7ba35e657ee88b25984c6e9	nba直播	4	1	http://www.hoopchina.com/tv
        String[] words = line.split("\t");
        if (words.length == 6) {
            context.write(new Text(words[0].substring(6,13)), new IntWritable(1));
        } else {
            counter.increment(1L);
        }
    }
}
