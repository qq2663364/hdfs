package com.hadoop.PVCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0016 11:50
 */
public class PVCountCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable outPutKey = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //用于计数各个省份的人数
        int count = 0;

        //遍历数组把上网数进行累计求和
        for (IntWritable value : values) {
            count += value.get();
        }

        //把省份ID及上网人数进行输出
        outPutKey.set(count);
        context.write(key,outPutKey);
    }
}
