package Refactoring.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class userDetailMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineString = value.toString();
		try {
			String[] dataList = lineString.split("\005");
			String time = dataList[1];
			String hour = time.split("T")[1].split(":")[0];
			String dtd = time.split("T")[0];
			String ip = dataList[2];
			String webSite = dataList[3];
			String tdt = dataList[9];
			String uc = dataList[10];
			String cuc = dataList[11];
			String vuc = dataList[12];
			String usag = dataList[14];
			String fs = dataList[15];
			String rf = dataList[16];
			String ps = dataList[17];
			String pu = dataList[18];
			String kw = dataList[19];
			String lenovoid = dataList[45];
			String FMSRC = dataList[53];
			String MSRC = dataList[54];
			String MSCH = dataList[55];
			String rc = dataList[57];
			Integer ref_type;
			String imei = "";

			//时间戳
			String date = time.split("\\+")[0].replace("T", " ");
			String timeStamp2 = tools.date2TimeStamp(date, "yyyy-MM-dd HH:mm:ss"); 
			System.out.println("date: " + date);
			

			// 获取Website_cd
			String website_cd = null;
			website_cd = getField.getWebsite_cd(webSite, ps, pu);

			// 获取浏览器信息和操作系统信息
			UserAgent userAgent = UserAgent.parseUserAgentString(usag);
			String browser = userAgent.getBrowser().toString().toLowerCase();
			String os = userAgent.getOperatingSystem().toString().toLowerCase();

			// 判断是否从app访问，并获取imei号
			if (usag.contains("lenovomallapp")) {
				if (os.contains("ios")) {
					tdt = "IOS";
				} else if (os.contains("android")) {
					tdt = "android";
				}
				Pattern pattern = Pattern.compile("(.*)(/lenovomallapp/\\d+_\\d+).*");
				Matcher matcher = pattern.matcher(usag);
				if (matcher.find()) {
					// System.out.println("Found value: " + matcher.group(2) );
					String appString = matcher.group(2);
					String[] appList = appString.split("/")[2].split("_");
					imei = appList[1];

				}
			}

			// 获取terminal_cd
			String terminal_cd = "";
			terminal_cd = getField.getTerminal_cd(webSite, ps, pu, tdt);
			System.out.println(webSite + ", " + ps + ", " + pu + ", " + tdt);
			System.out.println("terminal_cd: " + terminal_cd);

			// 根据ip获取国家和省市
			String country = "";
			String province = "";
			String city = "";
			HashMap<String, String> locationMap = Ip2province.IP2province(ip);
			country = locationMap.get("country");
			province = locationMap.get("province");
			city = locationMap.get("city");

			// 判断是否回访

			// 设置主键
			String keyString = dtd + "&" + hour + "&" + cuc + "&" + website_cd;

			ArrayList valueList = new ArrayList<String>();
			valueList.add(timeStamp2);
			valueList.add(time);
			valueList.add(dtd);
			valueList.add(hour);
			valueList.add(ip);
			valueList.add(uc);
			valueList.add(cuc);
			valueList.add(vuc);
			valueList.add(website_cd);
			valueList.add(terminal_cd);
			valueList.add(fs);
			valueList.add(rf);
			valueList.add(ps);
			valueList.add(pu);
			valueList.add(imei);
			valueList.add(rc);
			valueList.add(lenovoid);
			valueList.add(country);
			valueList.add(province);
			valueList.add(city);
			valueList.add(kw);
			valueList.add(MSCH);

			// 设置value1
			String valueString = tools.join(valueList, "\005");

			context.write(new Text(keyString), new Text(valueString));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
