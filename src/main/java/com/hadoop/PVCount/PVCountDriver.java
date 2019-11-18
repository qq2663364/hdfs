package com.hadoop.PVCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0016 10:47
 */
public class PVCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //指定输入和输出的路径
        args = new String[]{
                "hdfs://linux01:8020/pvcount/input",
                "hdfs://linux01:8020/pvcount/output6"
        };

        //构建默认配置文件类的对象
        Configuration conf = new Configuration();

        //获取一个job实例对象
        Job job = Job.getInstance(conf);
        job.setJarByClass(PVCountDriver.class);

        //设置输入路径和输出路径
        Path path = new Path(args[0]);
        Path path1 = new Path(args[1]);
        FileInputFormat.addInputPath(job,path);
        FileOutputFormat.setOutputPath(job,path1);
        //指定使用自己的mapper和reducer类
        job.setMapperClass(PVCountMapper.class);
        job.setReducerClass(PVCountReducer.class);
        //指定map输出的<k,v>键值对类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定reduce输出的<k,v>类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //指定自定分区class
        job.setPartitionerClass(PVCountPartitioner.class);
        //设置分区数,如果设置为0代表没有Reduce
        job.setNumReduceTasks(3);

        //设置预聚合
        job.setCombinerClass(PVCountReducer.class);

        //提交到yarn上运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
