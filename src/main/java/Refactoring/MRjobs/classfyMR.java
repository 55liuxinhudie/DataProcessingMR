package Refactoring.MRjobs;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import Refactoring.Common.getFromConfig;
import Refactoring.MRjobs.MulOutput.MapClass;
import Refactoring.Mapper.classifyMapper;
import Refactoring.Mapper.filterMapper;
import Refactoring.Reducer.LogReducer;
import Refactoring.Reducer.classfyReducer;
import Refactoring.Reducer.classifyMulReducer;
import Refactoring.Mapper.classifyMulMapper;

public class classfyMR {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if (args.length != 2){
			System.out.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"classfyMR");
		job.setJarByClass(classfyMR.class);
		job.setJobName("classfyMR");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setMapperClass(classifyMulMapper.class);
		job.setMapperClass(classifyMapper.class);
		job.setReducerClass(classifyMulReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		System.out.println("=====> This is classfyMR" );
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}