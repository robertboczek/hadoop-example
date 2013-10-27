package com.kucowka;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountExample extends Configured implements Tool {

	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {

		int result = ToolRunner.run(new Configuration(), new WordCountExample(), args);
		
		System.exit(result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		JobConf jobConf = new JobConf(conf, WordCountExample.class);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.setInputPaths(jobConf, in);
		FileOutputFormat.setOutputPath(jobConf, out);
		
		jobConf.setJobName("WordCount");
		jobConf.setMapperClass(MapClass.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.set("textoutputformat.separator", " : ");
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(jobConf);
		
		return 0;
	}

}
