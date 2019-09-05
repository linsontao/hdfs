package com.kaikeba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ListDirAllFile {
    public static void main(String[] args) {
        try {
            Path path = new Path("/test");
            printFiles(path);

        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void printFiles(Path path) throws IOException {
        String url = "hdfs://h1:8020";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        FileStatus[] status = fs.listStatus(path);
        for (FileStatus status1 : status) {
            if (status1.isFile()) {
                System.out.println(status1.getPath().toString());
            }
            if (status1.isDirectory()) {
                System.out.println(status1.getPath().toString());
                printFiles(status1.getPath());
            }
        }
    }
}
