package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Autor sc
 * @DATE 0010 11:40
 * KEYIN:输入的key值,输入字节偏移量,所以使用Long类型,对应hadoop的数据类型LongWritable
 * VALUEIN:输入的value值,即每行文本内容,所以使用String类型,对应hadoop的数据类型Text
 *
 *KEYOUT:输出的key值,即mao处理完的数据,对应Text(单词)
 * VALUEOUT:输出的value值,即map处理完的value,对应IntWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    /**
     * <hadoop spark > -> <0,"hadoop spark">
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private IntWritable outputValue = new IntWritable(1);
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //把Text类型转换为String类型,便于后面字符串操作
        String line = value.toString();
        //根据空格对line进行分割,然后得到的每个单词存放到String[]里
        //"hadoop spark"  -> ["hadoop","spark"]
        String[] words = line.split(" ");
        //遍历数组,拿到单词内容,组成键值对,并传递
        //["hadoop","spark"] -> ("hadoop",1)("spark",1)
        for(String word:words){
            outputKey.set(word);
            context.write(outputKey,outputValue);
        }
    }
}
