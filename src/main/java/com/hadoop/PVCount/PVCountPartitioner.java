package com.hadoop.PVCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Autor sc
 * @DATE 0016 11:19
 */
public class PVCountPartitioner extends Partitioner<IntWritable,IntWritable> {
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        //转换类型
        int PrID = key.get();
        //定义分区,满足条件的分为一个分区
        int partition = 2;

        if (PrID >= 0 && PrID < 10){
            partition = 0;
        }else if (PrID >= 10 && PrID < 20){
            partition = 1;
        }
        //返回分区
        return partition;
    }
}
