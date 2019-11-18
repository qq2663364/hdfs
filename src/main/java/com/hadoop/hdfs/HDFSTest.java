package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0009 16:14
 */
public class HDFSTest {

    private FileSystem fs;

    @Before
    public void init() throws IOException {
        //配置hadoop默认参数
        Configuration conf = new Configuration();
        //这里指定使用的是hdfs文件系统
        conf.set("fs.defaultFS","hdfs://linux01:8020");
        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME","hadoop01");
        //获得文件系统的操作对象
        fs = FileSystem.get(conf);
    }

    @Test
    public void testUpLoad() throws IOException {
        fs.copyFromLocalFile(new Path("D:/linux01.txt"),new Path("/"));
        fs.close();
    }
}
