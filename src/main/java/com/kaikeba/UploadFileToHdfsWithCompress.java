package com.kaikeba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static java.net.URI.create;


public class UploadFileToHdfsWithCompress {
    public static void main(String[] args) {
        //本地文件路径
        String src_filename = "/Users/lintao/medicalserver-logs-2.log";
        //HDFS文件路径
        String dest_filename = "hdfs://h1:8020/medicalserver-compress.log";
        try {
            //创建输入流
            String codeClassName = "org.apache.hadoop.io.compress.BZip2Codec";
            Class<?> codecClass = Class.forName(codeClassName);
            InputStream input = new BufferedInputStream(new FileInputStream(src_filename));
            //创建配置文件对象
            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

            FileSystem fs = FileSystem.get(create(dest_filename), conf);
            OutputStream output = fs.create(new Path(dest_filename));
            CompressionOutputStream compressOut = codec.createOutputStream(output);
            IOUtils.copyBytes(input, compressOut, 4096, true);
        } catch (Exception e) {
            System.out.println("Exception");
            e.printStackTrace();
        }
    }
}
