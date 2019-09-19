package com.kaikeba.mr.custompartitions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataCleanMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //开启map输出进行压缩的功能
        conf.set("mapreduce.map.output.compress", "true");
        //设置map输出的压缩算法是：BZip2Codec，它是hadoop默认支持的压缩算法，且支持切分
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //开启job输出压缩功能
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        //指定job输出使用的压缩算法
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        Job job = Job.getInstance(conf,DataCleanMain.class.getSimpleName());
        job.setJarByClass(DataCleanMain.class);
        //设置输入输出文件
        FileInputFormat.setInputPaths(job, new Path("hdfs://h1:8020/head100.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://h1:8020/head100outputcompress"));
        //设置map reduce 类
        job.setMapperClass(DataCleanMap.class);
        job.setReducerClass(DataCleanReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置combine
        job.setCombinerClass(DataCleanReduce.class);
        //设置自定义分区
        job.setPartitionerClass(CustomPartitions.class);
        job.setNumReduceTasks(4);
        job.waitForCompletion(true);

    }
}
