package com.kaikeba.mr.dataclean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AdjustOrderMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, AdjustOrderMap.class.getSimpleName());
        job.setJarByClass(AdjustOrderMain.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AdjustOrderMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        //job.setMapperClass(AdjustOrderMap.class);
        //job.setOutputKeyClass(IntWritable.class);
        //job.setOutputValueClass(Text.class);

        // 提交作业
        job.waitForCompletion(true);


    }
}
