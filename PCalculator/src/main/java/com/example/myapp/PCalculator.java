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
import java.util.Iterator;
import java.util.StringTokenizer;

public class PCalculator {

	private static long N = 23260642968L;

    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {
        //TODO: change everything to triGram
//        private Triple<String,String,String> triGram;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long n0=0, n1=0, t0=0, t1=0;
            for(Text t:values){
                StringTokenizer itr = new StringTokenizer(t.toString());
                String[] arr = itr.nextToken().split(",");
                if (arr[0].equals("t0")){
                    t0 = Long.parseLong(arr[1]);
                }
                else if(arr[0].equals("t1")){
                    t1 = Long.parseLong(arr[1]);
                }
                else if (arr[0].equals("n0")){
                    n0 = Long.parseLong(arr[1]);
                }
                else if (arr[0].equals("n1")){
                    n1 = Long.parseLong(arr[1]);
                }
                else if (arr[0].equals("p")){
                    context.write(key, t);
                    return;
                }
            }
            double p = ((t0 + t1) * 1.0) / (1.0 * N * (n0 + n1));;
            context.write(key, new Text("p," + p));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    private static void pCalculatorJob(String input1, String input2, String input3, String input4, String output) throws  IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pCalculator");
        job.setJarByClass(PCalculator.class);
        job.setMapperClass(PCalculator.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(PCalculator.PartitionerClass.class);
        job.setReducerClass(PCalculator.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input1)); //"/home/ohalf/Desktop/myapp/calculated/TRs/0/part-r-00000"
        FileInputFormat.addInputPath(job, new Path(input2)); //"/home/ohalf/Desktop/myapp/calculated/TRs/1/part-r-00000"
        FileInputFormat.addInputPath(job, new Path(input3)); //"/home/ohalf/Desktop/myapp/calculated/NRs/0/part-r-00000"
        FileInputFormat.addInputPath(job, new Path(input4)); //"/home/ohalf/Desktop/myapp/calculated/NRs/1/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/P"
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        pCalculatorJob(args[1], args[2], args[3], args[4], args[5]);
    }
}
