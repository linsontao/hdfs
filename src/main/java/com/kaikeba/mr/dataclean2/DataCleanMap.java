package com.kaikeba.mr.dataclean2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataCleanMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        NullWritable nullWritable = NullWritable.get();
        String[] word = line.split("\t");
        if (word.length == 6) {
            if (2 < Integer.valueOf(word[3]) && word[5].startsWith("http://zhidao.baidu.com")) {
                context.write(new Text(line), nullWritable);

            }
        }

    }
}
