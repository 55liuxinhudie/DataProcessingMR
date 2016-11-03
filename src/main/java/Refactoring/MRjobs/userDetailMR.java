package Refactoring.MRjobs;

import java.io.File;  
import java.io.IOException;  
import java.util.HashSet;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

import Refactoring.Mapper.userDetailMapper;
import Refactoring.Mapper.userDetailTest;
import Refactoring.Reducer.userDetailReducer;
import Refactoring.Reducer.userDetailReducerTest;

public class userDetailMR {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if (args.length != 2){
			System.out.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"userDetailMR");
		job.setJarByClass(userDetailMR.class);
		job.setJobName("userDetailMR");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(userDetailMapper.class);
		//job.setMapperClass(userDetailTest.class);
		job.setReducerClass(userDetailReducer.class);
		//job.setReducerClass(userDetailReducerTest.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.out.println("=====> This is LogAnalysis" );
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
