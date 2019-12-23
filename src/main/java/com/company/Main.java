package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output\\doc2.txt");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        HashMap<String,String> allUsers = new HashMap<String, String>();
            Job job = Job.getInstance();
            job.setJarByClass(Main.class);
            TextInputFormat.addInputPath(job, new Path("input\\logs_example.csv"));
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(LoginMapper.class);
            job.setReducerClass(LoginReducer.class);
            TextOutputFormat.setOutputPath(job, new Path("output\\doc2.txt"));
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] kosmos = value.toString().split(",");
            //context.write(new Text(kosmos[4]),new Text(kosmos[2]));
            context.write(new Text(kosmos[2]),new Text(kosmos[4]));

        }
    }
    public static class LoginReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            HashSet<String> tempString = new HashSet<>();
            for(Text value:values){
                tempString.add(value.toString());
            }
            context.write(key,new Text(tempString.toString()));

        }
    }
}
