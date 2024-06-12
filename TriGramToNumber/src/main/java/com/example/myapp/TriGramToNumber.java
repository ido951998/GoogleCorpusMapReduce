package com.example.myapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import software.amazon.awssdk.regions.Region;

public class TriGramToNumber {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text word = new Text();
        private final LongWritable occurrences = new LongWritable();
        private final Set<String> stopWords = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException {
            S3Tool s3Tool = new S3Tool("projoct2dsp2023", Region.US_EAST_1);
            BufferedReader reader = s3Tool.download_file("eng_stopwords.txt");
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                stopWords.add(line);
            }
            reader.close();
        }

        private boolean checkForNotLetters(String[] arr) {
            for (String s:arr) {
                if (s.length() <= 1) return true;
                for (char a : s.toCharArray()) {
               if (!((a >= 'a' && a<= 'z') || (a >= 'A' && a <= 'Z'))) return true;
//                    if (!(a >= 'א' && a <= 'ת')) return true;
                }
            }
            return false;
        }

        //This mapper writes the occurrences and not just the 1
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String triGram = itr.nextToken() + "-" + itr.nextToken() + "-" + itr.nextToken();
            if (!checkForStopWords(triGram.split("-")) && !checkForNotLetters(triGram.split("-"))) {
                word.set(triGram + "," + (Math.random() < 0.5 ? "0" : "1"));
                itr.nextToken();
                occurrences.set(Long.parseLong(itr.nextToken()));
                context.write(word, occurrences);
            }
        }

        private boolean checkForStopWords(String[] triGramArray) {
            for (String s:triGramArray){
                if (stopWords.contains(s)){
                    return true;
                }
            }
            System.err.println(Arrays.toString(triGramArray));
            return false;
        }
    }

    //Also sums
    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    private static void triGramToNumberJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "triGramToNumber");
        job.setJarByClass(TriGramToNumber.class);
        job.setMapperClass(TriGramToNumber.MapperClass.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(TriGramToNumber.ReducerClass.class);
        job.setReducerClass(TriGramToNumber.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        triGramToNumberJob(args[1], args[2]);
    }
}