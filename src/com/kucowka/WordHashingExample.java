package com.kucowka;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordHashingExample extends Configured implements Tool {

	public static class MapClass2 extends MapReduceBase
		implements Mapper<LongWritable, Text, NullWritable, Text> {
		
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(NullWritable.get(), word);
			}
		}
	}
	
	public static class PartitionByFirstLetter 
		extends MultipleTextOutputFormat<NullWritable, Text> {
		
		public String generateFileNameForKey(NullWritable key, Text value, String fileName) {
			Character c = Character.toLowerCase(value.toString().charAt(0));
			String prefix = null;
			if (c.compareTo('a') >= 0 && c.compareTo('z') <= 0) {
				prefix = String.valueOf(c);
			} else {
				prefix = "other";
			}
			
			return prefix + "/" + fileName;
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
		jobConf.setMapperClass(MapClass2.class);
		
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(PartitionByFirstLetter.class);
		jobConf.set("textoutputformat.separator", " : ");
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		jobConf.setNumMapTasks(4);
		jobConf.setNumReduceTasks(0);
		
		JobClient.runJob(jobConf);
		
		return 0;
	}

}

