package com.kaikeba;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import static java.net.URI.*;


public class UploadFileToHdfs {
    public static void main(String[] args) throws IOException {
        //本地文件路径
        String src_filename = "/Users/lintao/en5tcpdump.cacp";
        //HDFS文件路径
        String dest_filename = "hdfs://h1:8020/en5tcpdump.cacp";
        try {
            //创建输入流
            InputStream input = new BufferedInputStream(new FileInputStream(src_filename));
            //创建配置文件对象
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(create(dest_filename), conf);
            OutputStream output = fs.create(new Path(dest_filename));
            IOUtils.copyBytes(input, output, 4096, true);
        } catch (FileNotFoundException e) {
            System.out.println("Exception");
            e.printStackTrace();
        }
    }
}
