package Refactoring.Mapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Refactoring.Common.config;
import Refactoring.Common.getFromData;
import Refactoring.Common.tools;

public class classifyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		context.write(value, new IntWritable());
	}

	
}