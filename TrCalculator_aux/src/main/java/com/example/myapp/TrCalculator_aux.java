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

public class TrCalculator_aux {

    public static class MapperClass extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        //TODO: change everything to triGram
        @Override
        public void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        private int corpusNumFrom;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String corpusNumFrom_str = context.getConfiguration().get("corpusNumFrom","2");
            corpusNumFrom = Integer.parseInt(corpusNumFrom_str);
        }
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable l : values) {
                sum += l.get();
            }
            context.write(key, new Text("t" + corpusNumFrom + "," + sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void tRCalculator_auxJob(String input, String output, String corpusNumFrom) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("corpusNumFrom", corpusNumFrom);
        Job job = Job.getInstance(conf, "tRCalculator_aux");
        job.setJarByClass(TrCalculator_aux.class);
        job.setMapperClass(TrCalculator_aux.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(TrCalculator_aux.PartitionerClass.class);
        job.setReducerClass(TrCalculator_aux.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input)); //"/home/ohalf/Desktop/myapp/TRs/" + corpusNumFrom + "/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output)); //"/home/ohalf/Desktop/myapp/calculated/TRs/" + corpusNumFrom
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        tRCalculator_auxJob(args[1], args[2], args[3]);
    }
}
