package com.example.myapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.StringTokenizer;

public class NrCalculator {

    public static class MapperClass extends Mapper<Text, LongWritable, LongWritable, Text> {
        //TODO: change everything to triGram
//        private Triple<String,String,String> triGram;
        private int corpusNum;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String corpusNum_str = context.getConfiguration().get("corpusNum", "2");
            corpusNum = Integer.parseInt(corpusNum_str);
        }

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String wordAndCorpus = itr.nextToken();
            String[] arr = wordAndCorpus.split(",");
            if (arr.length == 1) {
                context.write(value, new Text("!!-!!"));
            } else {
                int actualCorpusNum = Integer.parseInt(arr[1]);
                if (corpusNum == actualCorpusNum) {
                    context.write(value, new Text("n" + corpusNum + "," + "1"));
                }

            }
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, Text, LongWritable, Text> {
        private int corpusNum;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String corpusNum_str = context.getConfiguration().get("corpusNum", "2");
            corpusNum = Integer.parseInt(corpusNum_str);
        }
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            boolean found = false;
            for (Text value : values) {
                StringTokenizer itr = new StringTokenizer(value.toString());
                String[] arr = itr.nextToken().split(",");
                if (arr.length == 1)
                    found = true;
                else
                    sum += Long.parseLong(arr[1]);
            }
            if (found)
                context.write(key, new Text("n" + corpusNum + "," + sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void nRCalculatorJob(String input1,String input2, String output, String corpusNum) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("corpusNum", corpusNum);
        Job job = Job.getInstance(conf, "nRCalculator");
        job.setJarByClass(NrCalculator.class);
        job.setMapperClass(NrCalculator.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(NrCalculator.PartitionerClass.class);
        job.setReducerClass(NrCalculator.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input1));//"/home/ohalf/Desktop/myapp/triGramToNumber/part-r-00000"
        FileInputFormat.addInputPath(job, new Path(input2));//"/home/ohalf/Desktop/myapp/calculated/WordsToR/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output));//"/home/ohalf/Desktop/myapp/calculated/NRs/" + corpusNum)
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        nRCalculatorJob(args[1], args[2], args[3], args[4]);
    }
}
