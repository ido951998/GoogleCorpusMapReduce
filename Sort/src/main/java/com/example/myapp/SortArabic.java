package com.example.myapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;


import java.io.IOException;
import java.util.StringTokenizer;

public class SortArabic {

    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, Text> {
        private static String numberSplitter(double val){
            String val_str = String.valueOf(val);
            String[] arr = val_str.split("-");
            if (arr.length == 1){
                return "a" + (100-Double.parseDouble(arr[0]));
            }
            else{
                String colons = "";
                for (int i=0; i<arr[1].length(); i++) colons += ":";
                return "b" + colons + arr[1] + (10-Double.parseDouble(arr[0].split("E")[0]));
            }
        }
        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String[] arr = itr.nextToken().split("-");
            if (arr.length == 3) {
                String key_str = arr[0] + "!!!" + arr[1] + "!!!" + numberSplitter(value.get());
                context.write(new Text(key_str), new Text(arr[2] + "!!!" + value.get()));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String[] arr = itr.nextToken().split("!!!");
            for(Text t:values){
                StringTokenizer itr2 = new StringTokenizer(t.toString());
                String[] arr2 = itr2.nextToken().split("!!!");
                context.write(new Text(arr[0]+"-"+arr[1]+"-"+arr2[0]), new DoubleWritable(Double.parseDouble(arr2[1])));
            }
        }
    }



    public static class MapperClassDecoder extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        //TODO: change everything to triGram
        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClassDecoder extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            for (DoubleWritable value:values){
                context.write(key, value);
            }
        }
    }


    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return 0;
        }
    }

    public static void SortArabicJob(String input1, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(SortArabic.class);
        job.setMapperClass(SortArabic.MapperClass.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(SortArabic.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input1)); //"/home/ohalf/Desktop/myapp/P/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/calculated/WordsToP"
        job.waitForCompletion(true);
    }

    public static void DecoderJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordsToP");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.MapperClassDecoder.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(WordsToP.PartitionerClass.class);
        job.setReducerClass(Sort.ReducerClassDecoder.class);
        job.setMapOutputKeyClass(Text.class);
//        job.setSortComparatorClass(TextComparator.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input)); //"/home/ohalf/Desktop/myapp/P/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/calculated/WordsToP"
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        SortArabicJob(args[1], args[2]);
//        DecoderJob("/home/ohalf/Desktop/final/part-r-00000", "/home/ohalf/Desktop/final/Decoded2");
        SortArabicJob("/home/ohalf/Desktop/final/part-r-00000", "/home/ohalf/Desktop/final/Decoded");
    }
}
