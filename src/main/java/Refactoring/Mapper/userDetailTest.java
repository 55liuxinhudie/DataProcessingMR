package Refactoring.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import Refactoring.Common.Ip2province;
import Refactoring.Common.getField;
import Refactoring.Common.tools;
import eu.bitwalker.useragentutils.UserAgent;
import java.lang.Math;

public class userDetailTest extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineString = value.toString().trim();
		try {
			String[] dataList = lineString.split("\005");
			String time = dataList[1];
			String hour = time.split("T")[1].split(":")[0];
			String ip = dataList[2];
			String webSite = dataList[3];
			Integer len = dataList.length;
			Random ran =  new Random();
			Integer r = ran.nextInt();
			if (r %2 == 0){
				len = r;
			}
		    String keyString = time + "+" + hour + "+" + ip + "+" + webSite;
			//String keyString = time + "+" + hour + "+" + ip;
			String valueString = "test";
			
			context.write(new Text(keyString), new IntWritable(len));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
