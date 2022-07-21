package net.togogo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceDemo extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;
        for(IntWritable i:values){
            //将value 进行累加
            count+=i.get();
        }
        Text keyOut = new Text(key);
        IntWritable valueOut = new IntWritable(count);

        context.write(keyOut,valueOut);
    }
}
