package com.hadoop.PVCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0016 10:04
 */
public class PVCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    /**
     * <LongWritable, Text, IntWritable,IntWritable> -> <偏移量,每行数据,省份ID,1></>
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    private IntWritable outPutKey = new IntWritable();
    private IntWritable outPutValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将每行转换成String,便于后续对字符串的处理
        String line = value.toString();

        //根据指标符分割每行文本
        String[] files = line.split("\t");

        //如果字段丢失太多,那么此条记录作废
        if (files.length < 30) {
            context.getCounter("web-pv","length less 30").increment(1);
            return;
        }

        //从字段数组中获取省份ID
        String provinceID = files[23];
        //对省份字段验证判断,是否是一个int类型
        Integer provinceIDtest = Integer.MIN_VALUE;
        try {
            provinceIDtest = Integer.valueOf(provinceID);
        } catch (Exception e) {
            context.getCounter("web-pv","proviceID not number").increment(1);
            return;
        }

        //把省份ID作为key进行输出
        //强制转换类型
        outPutKey.set(provinceIDtest);
        context.write(outPutKey, outPutValue);
    }
}
