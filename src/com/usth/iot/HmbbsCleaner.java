package com.usth.iot;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HmbbsCleaner {

	public static void main(String[] args) throws Exception {
		final String INPUT_PATH = args[0];
		final String OUTPUT_PATH = args[1];
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		Job job = new Job(conf, HmbbsCleaner.class.getSimpleName());
		job.setJarByClass(HmbbsCleaner.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] parse = new LogParser().parse(line);
			//过滤掉静态信息
			if(parse[2].startsWith("GET /static/") || parse[2].startsWith("GET /uc_server")){
				return;
			}
			//过掉开头的特定格式字符串
			if (parse[2].startsWith("GET /")) {
				parse[2] = parse[2].substring("GET /".length());
			}else if (parse[2].startsWith("POST /")) {
				parse[2] = parse[2].substring("POST /".length());
			}
			//过滤结尾的特定格式字符串
			if (parse[2].endsWith(" HTTP/1.1")) {
				parse[2] = parse[2].substring(0, parse[2].length()-" HTTP/1.1".length());
			}
			v2.set(parse[0]+"\t"+parse[1]+"\t"+parse[2]);
			context.write(key, v2);
		}
	}
	
	public static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				context.write(v2, NullWritable.get());
			}
		}
	}
}