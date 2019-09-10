package com.gxh.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {
    FileSystem fs;
    @Before
    public void before() throws IOException, InterruptedException {
        //获取一个hdfs的抽象封装对象
       fs= FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "gxh");
        System.out.println("Before");
    }
    //上传
    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("D:/wa.txt"),new Path("/wa.txt"));
    }


    //下载
    @Test
    public void download() throws IOException, InterruptedException {

        //该对象操作文件系统
        fs.copyToLocalFile(new Path("/test"),new Path(("D:/test")));

    }
    //重命名
    @Test
    public void rename() throws IOException, InterruptedException {
        fs.rename(new Path("/test"),new Path("/test2"));
    }
    //删除
    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/wa.txt"), true);
        if(delete){
            System.out.println("删除成功！！！");
        }else{
            System.out.println("删除失败！！！");
        }
    }
    //追加
    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test2/1.txt"), 1024);
        FileInputStream open = new FileInputStream("D:/wa.txt");
        IOUtils.copyBytes(open,append,1024,true);
    }
    //查看
    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("以下信息是文件信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getPermission());
            }else{
                System.out.println("以下信息是文件夹信息");
                System.out.println(fileStatus.getPath());
            }
        }
    }
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            System.out.println("==========================");
            System.out.println(file.getPath());
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.print("块在");
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
        }
    }
    @After
    public void after() throws IOException {
        //关闭文件系统
        fs.close();
        System.out.println("After");
    }
}
