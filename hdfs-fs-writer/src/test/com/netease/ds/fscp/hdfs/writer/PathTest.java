package com.netease.ds.fscp.hdfs.writer;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class PathTest {

    @Test
    public void testPath() {
        Path targetPath = new org.apache.hadoop.fs.Path("/user/test/test1\\/");
        Path targetPath1 = new org.apache.hadoop.fs.Path("/user/test/test1");
        Path[] path = new org.apache.hadoop.fs.Path[3];
        path[0] = new Path("/user/test/test1//2/3");
        path[1] = new Path("/user/test/test1//3");
        path[2] = new Path("/user/test/test1/3");
        Assert.assertEquals(HdfsFileWriter.removePrefix(path[0].toString(), targetPath.toString()), "2/3");
        Assert.assertEquals(HdfsFileWriter.removePrefix(path[1].toString(), targetPath1.toString()), "/3");
        Assert.assertEquals(HdfsFileWriter.removePrefix(path[2].toString(), targetPath1.toString()), "/3");
        System.out.println(targetPath.toString());
        System.out.println(targetPath1.getParent());
    }
}
