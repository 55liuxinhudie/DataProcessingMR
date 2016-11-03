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

import Refactoring.Common.config;
import Refactoring.Common.getFromConfig;
import Refactoring.Common.getFromData;
import Refactoring.Common.tools;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineString = value.toString();
		ArrayList stringList = new ArrayList<String>();
		try {
			String[] dataList = lineString.split("\\t");
			String ipString = dataList[1];
			String timeString = dataList[0];
			String urlString = dataList[3].split(" ")[1];

			if (lineString.contains("te?WS=") || lineString.contains("te?AWS=")) {

				Map<String, String> dataMap = new HashMap<String, String>();
				dataMap = getFromData.getData(urlString.split("\\?")[1]);

				if (urlString.contains("AWS=")) {
					if (urlString.contains("AIP=")) {
						stringList.add("AIP");
						config configAIP = new config();
						configAIP.init("aip.properties");
						LinkedHashSet<String> Set = configAIP.getKeySet();
						stringList = tools.getList(stringList, dataMap, Set, timeString, ipString);
					} else if (urlString.contains("AEVT=")) {
						stringList.add("AEVT");
						config configAEVT = new config();
						configAEVT.init("aevt.properties");
						LinkedHashSet<String> Set = configAEVT.getKeySet();
						stringList = tools.getList(stringList, dataMap, Set, timeString, ipString);
					}
				} else {
					if (urlString.contains("CLE=")) {
						stringList.add("CLE");
						config configCLE = new config();
						configCLE.init("cle.properties");
						LinkedHashSet<String> Set = configCLE.getKeySet();
						stringList = tools.getList(stringList, dataMap, Set, timeString, ipString);

					} else {
						stringList.add("PV");
						config configPV = new config();
						configPV.init("pv.properties");
						LinkedHashSet<String> Set = configPV.getKeySet();
						stringList = tools.getList(stringList, dataMap, Set, timeString, ipString);
					}
				}

				String outputString = tools.join(stringList, "\005");
				// System.out.println(outputString);
				context.write(new Text(outputString), new IntWritable());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
