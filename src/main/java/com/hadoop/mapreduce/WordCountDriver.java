package com.hadoop.mapreduce;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0010 14:06
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //指定输入和输出路径
//        args = new String [] {
//                "hdfs://linux01:8020/wordcount/input/words.txt",
//                "hdfs://linux01:8020/wordcount/output13"
//        };

        //构建默认配置文件类的对象
        Configuration conf = new Configuration();

        //获取一个job的实例对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);

        //设置输入路径和输出路径
        Path path = new Path("hdfs://linux01:8020/wordcount/input/words.txt");
        Path path1 = new Path("hdfs://linux01:8020/wordcount/output");

        FileInputFormat.addInputPath(job,path);
        FileOutputFormat.setOutputPath(job,path1);

        //指定使用自己的mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定map输出的<key,value>键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定reduce输出的<key,value>键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setUser("hadoop01");
        //提交到yarn上运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
