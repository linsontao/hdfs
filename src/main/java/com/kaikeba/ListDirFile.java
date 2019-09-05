package com.kaikeba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

public class ListDirFile {
    public static void main(String[] args) {
        try {
            String url = "hdfs://h1:8020/test";
            listDirFile(url);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listDirFile(String url) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path Path = new Path(url);
        FileStatus[] status = fs.listStatus(Path);
        for (FileStatus status1 : status) {
            System.out.println(status1.getPath().toString());
            fs.close();
        }
    }
}
