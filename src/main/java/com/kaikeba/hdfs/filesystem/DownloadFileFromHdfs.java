package com.kaikeba.hdfs.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class DownloadFileFromHdfs {
    public static void main(String[] args) {
        //本地文件路径
        String local_filename = "/Users/lintao/fileFromHdfs.txt";
        //HDFS文件路径
        String hdfs_filename = "hdfs://h1:8020/test/output1/part-r-00000";
        //配置文件对象
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(hdfs_filename), conf);
            FSDataInputStream hdfsInputStream = fs.open(new Path(hdfs_filename));

            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(local_filename));
            IOUtils.copyBytes(hdfsInputStream, outputStream, 4096, true);
        } catch (IOException e) {
            System.out.println("Exception");
            e.printStackTrace();
        }
    }
}
