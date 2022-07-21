package net.togogo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class JobDemo {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            //设置参数
            job.setJarByClass(JobDemo.class);
            job.setMapperClass(MapDemo.class);
            job.setReducerClass(ReduceDemo.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置文件的路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            //设置结果输出的路径
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            //执行job
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
