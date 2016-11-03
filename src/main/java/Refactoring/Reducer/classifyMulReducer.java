package Refactoring.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class classifyMulReducer extends  Reducer<Text, IntWritable, NullWritable, Text> {

	private MultipleOutputs mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs(context);
	}
	public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated constructor stub
		mos.write(NullWritable.get(), key, generateFileName(key));
	}
	
	private String generateFileName(Text value) {
		String lineString = value.toString();
		String type = "ELSE";
		if (lineString.startsWith("PV")) {
			type = "PV";
		} else if (lineString.startsWith("CLE")) {
			type = "CLE";
		} else if (lineString.startsWith("AIP")) {
			type = "AIP";
		} else if (lineString.startsWith("AEVT")) {
			type = "AEVT";
		}else{
			type = "ELSE";
		}
		return type + "/";
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
	}
	
}
