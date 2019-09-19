package com.kaikeba.mr.custompartitions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitions extends Partitioner<Text, IntWritable> {

    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        int intKey = Integer.valueOf(key.toString());
        return intKey % numReduceTasks;
    }
}
