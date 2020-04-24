package studentdemo;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class stu {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String myString = value.toString(); 
			String[] studentscore = myString.split(",");
		
			if (studentscore[5].equals("YES") ) {	
				output.collect(new Text("Students passed"), one);
			}
		
		}  
	}

	// REDUCER 
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
	
	// DRIVER CONFIG
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(stu.class);
		conf.setJobName("StudentPass");
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
