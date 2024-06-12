package com.example.myapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class WordsToP {

    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {
        //TODO: change everything to triGram
//        private Triple<String,String,String> triGram;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double p = -1;
            List<String> list = new ArrayList<>();
            for (Text l : values){
                StringTokenizer itr = new StringTokenizer(l.toString());
                String[] arr = itr.nextToken().split(",");
                if (arr.length==2){
                    p = Double.parseDouble(arr[1]);
                    for (String w:list){
                        context.write(new Text(w), new DoubleWritable(p));
                    }
                }
                else if (p == -1){
                    list.add(arr[0]);
                }
                else{
                    context.write(new Text(arr[0]), new DoubleWritable(p));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void WordsToPJob(String input1, String input2, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordsToP");
        job.setJarByClass(WordsToP.class);
        job.setMapperClass(WordsToP.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(WordsToP.PartitionerClass.class);
        job.setReducerClass(WordsToP.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input1)); //"/home/ohalf/Desktop/myapp/P/part-r-00000"
        FileInputFormat.addInputPath(job, new Path(input2)); //"/home/ohalf/Desktop/myapp/calculated/RtoWords/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/calculated/WordsToP"
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        WordsToPJob(args[1], args[2], args[3]);
    }
}
