package com.kaikeba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class ListDirFile {
    public static void main(String[] args) throws IOException {
        Scanner listDir = new Scanner(System.in);
        System.out.print("请输入需要列出的目录：");
        System.out.println(listDir.nextLine());
        Configuration conf = new Configuration();
        String url = "hdfs://h1:10080/test";
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path Path = new Path(url);
        FileStatus[] status = fs.listStatus(Path);
        for (int i = 0; i < status.length; i++) {
                if (status[i].getPath().is)
                System.out.println(status[i].getPath().toString());
            }
        fs.close();

    }
}
