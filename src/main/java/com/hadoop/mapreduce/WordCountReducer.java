package com.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Autor sc
 * @DATE 0010 12:07
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 分组:相同的key的<key,value>键值对合并到一起</>
     * ("hadoop",1)("spark",1)
     * ("hadoop",1)("spark",1)
     * 分组后
     * <hadoop,[1,1]></>
     * <spark,[1,1]>
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private IntWritable outputValue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //获取到迭代对象
        Iterator<IntWritable> iterator = values.iterator();

        //统计单词数
        int wordSum = 0;
        while (iterator.hasNext()) {
            //获取迭代对象中的值
            IntWritable value = iterator.next();
            //value.get() 获取IntWritable中的整数值
            wordSum = wordSum + value.get();
        }
        outputValue.set(wordSum);
        //通过context传递出去,写到输入文件中
        context.write(key,outputValue);
    }
}
