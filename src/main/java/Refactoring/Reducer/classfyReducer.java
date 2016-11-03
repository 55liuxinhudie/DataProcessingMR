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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class classfyReducer extends  Reducer<Text, IntWritable, Text, IntWritable>  {
	private MultipleOutputs<Text,IntWritable> multipleOutputs;

	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
	  multipleOutputs  = new MultipleOutputs<Text, IntWritable>(context);
	 }
	 
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated constructor stub
		String lineString = values.toString();
		String type = "ELSE/";
		//String[] lineList = lineString.split("\005");
		
		if (lineString.startsWith("PV/")) {
			type = "PV/";
		} else if (lineString.startsWith("CLE")) {
			type = "CLE/";
		} else if (lineString.startsWith("AIP")) {
			type = "AIP/";
		} else if (lineString.startsWith("AEVT")) {
			type = "AEVT/";
		}else{
			type = "ELSE/";
		}
		multipleOutputs.write(type, values, null);
	}

	 @Override
	 protected void cleanup(Context context)
	   throws IOException, InterruptedException {
	  multipleOutputs.close();
	 }
	 
}