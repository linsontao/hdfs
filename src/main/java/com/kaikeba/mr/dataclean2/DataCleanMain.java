package com.kaikeba.mr.dataclean2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataCleanMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        //调用getInstance方法，生成job实例
        Job job = Job.getInstance(conf, DataCleanMain.class.getSimpleName());
        //设置jar包，参数是包含main方法的类
        job.setJarByClass(DataCleanMain.class);

        //设置MR的输入输出格式，默认是Text,如果一样则无需设置，这里是一样的。
        //        job.setInputFormatClass(TextInputFormat.class);
        //        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置处理Map阶段的自定义的类
        job.setMapperClass(DataCleanMap.class);

        //设置处理Reduce阶段的自定义类，这里没有

        //这里map阶段就直接输出了，所以要设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //这里没有Reduce，设置为0
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);

    }
}
