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

public class RToWords {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        //TODO: change everything to triGram
//        private Triple<String,String,String> triGram;
        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String word = itr.nextToken().split(",")[0];
            context.write(new Text(word), value);
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, LongWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long r = 0;
            for (LongWritable l : values){
                r += l.get();
            }
            context.write(new LongWritable(r), key);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void RtoWordsJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RtoWords");
        job.setJarByClass(RToWords.class);
        job.setMapperClass(RToWords.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(RToWords.PartitionerClass.class);
        job.setReducerClass(RToWords.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input)); //"/home/ohalf/Desktop/myapp/triGramToNumber/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/calculated/RtoWords"
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        RtoWordsJob(args[1], args[2]);
    }
}
