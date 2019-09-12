package com.kaikeba.mr.dataclean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AdjustOrderMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        NullWritable nullValue = NullWritable.get();
        String[] words = line.split("\t");
        if (words.length == 6) {
            if (words[5].endsWith(".html")) {
                String newLine = words[2] + "\t" + words[1] + "\t" + words[0] + "\t" + words[3] + "\t" + words[4] + "\t" + words[5];
                context.write(new Text(newLine),nullValue);
            }
        }
    }
}
