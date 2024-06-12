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

public class TrCalculator {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, Text> {
        //TODO: change everything to triGram
//        private Triple<String,String,String> triGram;
        private int corpusNumFrom;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String corpusNumFrom_str = context.getConfiguration().get("corpusNumFrom","2");
            corpusNumFrom = Integer.parseInt(corpusNumFrom_str);
        }
        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            String text = itr.nextToken();
            String actualCorpusNum_str = text.split(",")[1];
            if (corpusNumFrom == Integer.parseInt(actualCorpusNum_str)){
                context.write(new Text(text.split(",")[0]), new Text("0,"+value.get()));
            }
            else{
                context.write(new Text(text.split(",")[0]), new Text(String.valueOf(value.get())));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, LongWritable, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long r = 0;
            for (Text t : values) {
                StringTokenizer itr = new StringTokenizer(t.toString());
                String[] arr = itr.nextToken().split(",");
                if (arr.length == 2) r = Long.parseLong(arr[1]);
                sum += Long.parseLong(arr[0]);
//                r += Long.parseLong(arr[0]);
            }
            context.write(new LongWritable(sum+r), new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }



    public static void tRCalculatorJob(String input, String output, String corpusNumFrom) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("corpusNumFrom", corpusNumFrom);
        Job job = Job.getInstance(conf, "tRCalculator");
        job.setJarByClass(TrCalculator.class);
        job.setMapperClass(TrCalculator.MapperClass.class);
        job.setNumReduceTasks(1);
//        job.setPartitionerClass(TrCalculator.PartitionerClass.class);
        job.setReducerClass(TrCalculator.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));//"/home/ohalf/Desktop/myapp/triGramToNumber/part-r-00000"
        FileOutputFormat.setOutputPath(job, new Path(output));//"/home/ohalf/Desktop/myapp/TRs/" + corpusNumFrom
        job.waitForCompletion(true);
        //tRCalculator_auxJob(corpusNumFrom);
        //FileUtils.deleteDirectory(new File("/home/ohalf/Desktop/myapp/TRs/" + corpusNumFrom));
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        tRCalculatorJob(args[1],args[2],args[3]);
    }
}
