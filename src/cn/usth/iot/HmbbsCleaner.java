package cn.usth.iot;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 27.19.74.143 - - [30/May/2013:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
 */

public class HmbbsCleaner extends Configured implements Tool{
//	public static final String INPUT_PATH = "hdfs://hadoop0:9000/hmbbs_logs/";
//	public static final String OUT_PATH = "hdfs://hadoop0:9000/hmbbs_clean/";
	public static String INPUT_PATH = "";
	public static String OUTPUT_PATH = "";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new HmbbsCleaner(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		INPUT_PATH = args[0];
		OUTPUT_PATH = args[1];
		// TODO Auto-generated method stub
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
		return 0;
	}	
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] array = new LogParser().parse(v1.toString());
			//过滤掉静态信息
			if(array[2].startsWith("GET /static/") || array[2].startsWith("GET /uc_server")){
				return;
			}
			//过掉开头的特定格式字符串
			if (array[2].startsWith("GET /")) {
				array[2] = array[2].substring("GET /".length());
			}else if (array[2].startsWith("POST /")) {
				array[2] = array[2].substring("POST /".length());
			}
			//过滤结尾的特定格式字符串
			if (array[2].endsWith(" HTTP/1.1")) {
				array[2] = array[2].substring(0, array[2].length()-" HTTP/1.1".length());
			}
			context.write(k1, new Text(array[0]+"\t"+array[1]+"\t"+array[2]));
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
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
