package Refactoring.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

import Refactoring.Common.getField;
import Refactoring.Common.tools;

public class userDetailReducer extends  Reducer<Text, Text, Text, IntWritable>  {

	public void reduce(Text key, Iterable<Text> values,  
            Context context) throws IOException, InterruptedException {
		
		// TODO Auto-generated constructor stub
		try{
			ArrayList ucList = new ArrayList<String>();
			ArrayList vucList = new ArrayList<String>();
			ArrayList ipList = new ArrayList<String>();
			ArrayList tcdList = new ArrayList<String>();
			ArrayList imeiList = new ArrayList<String>();
			ArrayList lidList = new ArrayList<String>();
			ArrayList couList = new ArrayList<String>();
			ArrayList proList = new ArrayList<String>();
			ArrayList cityList = new ArrayList<String>();
			
			//把时间戳当key，并对map排序
			Map<String, String> valueMap = new TreeMap<String, String>(
	                new Comparator<String>() {
	                    public int compare(String obj1, String obj2) {
	                        // 降序排序
	                        return obj1.compareTo(obj2);
	                    }
	                });
			
			for (Text value: values){
				String vString = value.toString();
				String[] valueList = vString.split("\005");
				//System.out.println("size: " + valueList.length);
				String timeStamp = valueList[0];
				valueMap.put(timeStamp, vString);
				
				String ip = valueList[4];
				if (!ipList.contains(ip)){
					ipList.add(ip);
				}
				
				String uc = valueList[5];
				if (!ucList.contains(uc)){
					ucList.add(uc);
				}
				
				String vuc = valueList[7];
				if (!vucList.contains(vuc)){
					vucList.add(vuc);
				}
				
				String terminal_cd = valueList[9];
				if (!tcdList.contains(terminal_cd)){
					tcdList.add(terminal_cd);
				}
				
				String imei = valueList[14];
				if(!imeiList.contains(imei)){
					imeiList.add(imei);
				}
				
				String lenovoid = valueList[16];
				if(!lidList.contains(lenovoid)){
					lidList.add(lenovoid);
				}
				
				String country = valueList[17];
				if(!couList.contains(country)){
					couList.add(country);
				}
				
				String province = valueList[18];
				if(!proList.contains(province)){
					proList.add(province);
				}
				
				String city = valueList[19];
				if(!cityList.contains(city)){
					cityList.add(city);
				}
				
			}
			
			ArrayList valueList = new ArrayList<String>();
			for (Map.Entry<String, String> entry:valueMap.entrySet()){
				valueList.add(entry.getValue());
			}
			
			String firstValue = (String) valueList.get(0);
			String lastValue = (String) valueList.get(valueList.size()-1);
			
			String [] firstList = firstValue.split("\005");
			String [] lastList = lastValue.split("\005");
			//for(String v: firstList){
			//	System.out.println(v);
			//}
			
			String ftimeStamp     = firstList[0];
			String ftime	      = firstList[1];
			String fdtd	      	  = firstList[2];
			String fhour	      = firstList[3];
			String cuc 			  = firstList[6];
			String website_cd     = firstList[8];
			String ffs	          = firstList[10];
			String frf	          = firstList[11];
			String fps	          = firstList[12];
			String fpu	          = firstList[13];
			String frc	          = firstList[15];
			String fkw	          = firstList[20];
			String msch			  = firstList[21];
			
			String lps	          = lastList[11];
			String lpu	          = lastList[12];
			String lrc	          = lastList[14];
			String lkw	          = lastList[19];
		
			//生成相关字段数组
			String ucString = tools.join(ucList, ",");
			String vucString = tools.join(vucList, ",");
			String ipString = tools.join(ipList, ",");
			String tcdString = tools.join(tcdList, ",");
			String imeiString = tools.join(imeiList, ",");
			String lidString = tools.join(lidList, ",");
			String couString = tools.join(couList, ",");
			String proString = tools.join(proList, ",");
			String cityString = tools.join(cityList, ",");
			
			// 判断访问来源
			String refType = getField.getReftype(ffs, fps, website_cd, fkw, msch);
			//System.out.println(ffs + ", "+ frf + ", " + fps+ ", " +website_cd+ ", " +fkw);
			//System.out.println("refType: " + refType);
			//是否是回访
			String isNew = "0";
			if (!frc.equals("0")){
				isNew = frc;
			}
			
			//pv数
			String pvCount = String.valueOf(valueList.size());
			
			//拼写userDetail表需要的字段
			ArrayList<String> keyList = new ArrayList<String>();
			keyList.add(ftimeStamp);
			keyList.add(ftime);
			keyList.add(fdtd);
			keyList.add(fhour);
			keyList.add(ipString);
			keyList.add(ucString);
			keyList.add(cuc);
			keyList.add(vucString);
			keyList.add(website_cd);
			keyList.add(ffs);
			keyList.add(frf);
			keyList.add(fps);
			keyList.add(fpu);
			keyList.add(refType);
			keyList.add(lpu);
			keyList.add(lps);
			keyList.add(imeiString);
			keyList.add(lidString);
			keyList.add(couString);
			keyList.add(proString);
			keyList.add(cityString);
			keyList.add(pvCount);
			
			String keyString = tools.join(keyList, "\005");
			
			context.write(new Text(keyString), new IntWritable());
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}


