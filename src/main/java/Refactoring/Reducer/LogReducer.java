package Refactoring.Reducer;

import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

public class LogReducer extends  Reducer<Text, IntWritable, Text, IntWritable>  {

	public void reduce(Text key, Iterable<IntWritable> values,  
            Context context) throws IOException, InterruptedException {
		// TODO Auto-generated constructor stub
		context.write(key, null);
	}

}
