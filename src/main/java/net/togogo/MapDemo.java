package net.togogo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    java php mysql
    php java python
* */

//keyin  读取到的文本的偏移量  是long类型
//valuein 读取到的第一行的文本内容    text
//keyout map阶段处理完成之后输出的key text  <java,2> <php,2>
//valueout map阶段处理完成之后的value int
public class MapDemo extends Mapper<LongWritable, Text,Text, IntWritable> {
    //实现map方法

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //得到一行的文本  java php mysql
        String line = value.toString();
        //将文本进行拆分
        String[] s = line.split(" ");
        //遍历
        for(String word:s){
            Text keyOut =  new Text(word);
            IntWritable valueOut = new IntWritable(1);
            //将数据发送给reduce
            context.write(keyOut,valueOut);
            //<java,1><php,1><mysql,1>
            // <java,1><php,1><python,1>
        }
    }
}
