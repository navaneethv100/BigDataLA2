package studentdemo;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class stuabove60 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String myString = value.toString(); 
			String[] studentscore = myString.split(",");
			int score = Integer.parseInt(studentscore[2]);
			if (score > 60) {
				output.collect(new Text(" Score Above 60 "), one);
			}
		}  
	}

	// REDUCER CODE
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
			int count = 0 ; 
			Text s_key = key ; 
			while(values.hasNext()) {
				IntWritable value = values.next(); 
				count += value.get();
			}
			output.collect(s_key, new IntWritable(count));
			}
	}
	
	// DRIVER CODE
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(stuabove60.class);
		conf.setJobName("Studentmarks");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
}
