package Refactoring.MRjobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import junit.framework.Test;
import net.sf.json.JSONObject;
import Refactoring.Common.getFromConfig;
import Refactoring.Common.getField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import Refactoring.Common.config;
import Refactoring.Common.getFromData;
import Refactoring.Common.tools;
import Refactoring.Common.Ip2province;
import Refactoring.Common.tools;
import java.util.Random;

public class test {

	private static String outputString;

	public static void run() throws IOException {
		String file = "src/main/resources/test.txt";
		RandomAccessFile raf = new RandomAccessFile(new File(file), "r");
		String s;

		while ((s = raf.readLine()) != null) {
			// String lineString = "2016-08-29T09:57:58+08:00 221.193.207.66
			// Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML,
			// like Gecko) Chrome/42.0.2311.154 Safari/537.36 LBBROWSER GET
			// /te?WS=10000001&SWS=&SWSID=&SWSPID=&RD=record&TDT=web&UC=_ck15112710365019515084107737751&CUC=_sk201607020350000.11583100.8255&VUC=2016-08-291472433334018&LUC=_ck15112710365019515084107737751&UN=&UID=1.zwXXZG4yQBs&RC=37&PS=www.lenovo.com.cn&PU=%2F&FS=&RF=&SW=1349&SCW=1349&SCH=585&SSH=6280&BR=chrome&LTL=32&MSRC=&EDM=&CLE=||show::goodsid::A0000::latag_pc_home_like_rec_1_A0000_53011_topn%2Clatag_pc_home_like_rec_2_A0000_53170_topn%2Clatag_pc_home_like_rec_3_A0000_52413_topn%2Clatag_pc_home_like_rec_4_A0000_52158_topn%2Clatag_pc_home_like_rec_5_A0000_51494_topn%2Clatag_pc_home_like_rec_6_A0000_52665_topn%2C||&BUID=&MID=1472435871916&LASPID=&random=0.4538382049649954
			// HTTP/1.1";
			String lineString = s;
			System.out.println(lineString);
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

					System.out.println("size: " + stringList.size());
					String outputString = tools.join(stringList, "\005");
					System.out.println(outputString);
					System.out.println("length: " + outputString.split("\005").length);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		raf.close();
	}

	private static void run1() {
		String value = "3070808,1963,1096,,\"US\",\"IA\",,1,,623,3,39,,4,,0.375,,,,,,,";
		String[] split = value.toString().split(",", -1);
		System.out.println(split[4]);
		String country = split[4].substring(1, 3);
		System.out.println(country + "/");
	}

	private static void userDetail() throws IOException {
		String file = "src/main/resources/test.txt";
		RandomAccessFile raf = new RandomAccessFile(new File(file), "r");
		String s;

		while ((s = raf.readLine()) != null) {

			// String lineString = "2016-08-29T09:57:58+08:00 221.193.207.66
			// Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML,
			// like Gecko) Chrome/42.0.2311.154 Safari/537.36 LBBROWSER GET
			// /te?WS=10000001&SWS=&SWSID=&SWSPID=&RD=record&TDT=web&UC=_ck15112710365019515084107737751&CUC=_sk201607020350000.11583100.8255&VUC=2016-08-291472433334018&LUC=_ck15112710365019515084107737751&UN=&UID=1.zwXXZG4yQBs&RC=37&PS=www.lenovo.com.cn&PU=%2F&FS=&RF=&SW=1349&SCW=1349&SCH=585&SSH=6280&BR=chrome&LTL=32&MSRC=&EDM=&CLE=||show::goodsid::A0000::latag_pc_home_like_rec_1_A0000_53011_topn%2Clatag_pc_home_like_rec_2_A0000_53170_topn%2Clatag_pc_home_like_rec_3_A0000_52413_topn%2Clatag_pc_home_like_rec_4_A0000_52158_topn%2Clatag_pc_home_like_rec_5_A0000_51494_topn%2Clatag_pc_home_like_rec_6_A0000_52665_topn%2C||&BUID=&MID=1472435871916&LASPID=&random=0.4538382049649954
			// HTTP/1.1";
			String lineString = s;
			String[] dataList = lineString.split("\\005");
			System.out.println("dataList.lenth: " + dataList.length);
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

			// 时间戳
			String date = time.split("\\+")[0].replace("T", " ");
			String timeStamp2 = tools.date2TimeStamp(date, "yyyy-MM-ddHH:mm:ss");
			System.out.println("date: " + date);
			System.out.println("timeStamp2: " + timeStamp2);

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
			String key = dtd + "&" + hour + "&" + cuc + "&" + website_cd;

			ArrayList valueList = new ArrayList<String>();
			valueList.add(dtd);
			valueList.add(hour);
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

			// 设置value1
			String value = tools.join(valueList, "\005");

			System.out.println("key: " + key);
			System.out.println("value: " + value);
			// break;

		}

	}
	
	public static void userDetailReducer() throws IOException{
		String file = "src/main/resources/test.txt";
		RandomAccessFile raf = new RandomAccessFile(new File(file), "r");
		String s;

		while ((s = raf.readLine()) != null) {
			String values = s;
			
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
			for (String value: values.split("\\|\\|")){
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
			
			String keyString = tools.join(keyList, "|");
			
			for(String key:keyList){
				System.out.println(key);
			}
			System.out.println(keyString);
			break;
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		 //run();
		
		//userDetail();
		userDetailReducer();
		
		
    }
	

	private static HashMap<String, String> toHashMap(Object object) {
		HashMap<String, String> data = new HashMap<String, String>();
		// 将json字符串转换成jsonObject
		JSONObject jsonObject = JSONObject.fromObject(object);
		Iterator it = jsonObject.keys();
		// 遍历jsonObject数据，添加到Map对象
		while (it.hasNext()) {
			String key = String.valueOf(it.next());
			if (key.equals("country") || key.equals("province") || key.equals("city")) {
				String value = (String) jsonObject.get(key);
				data.put(key, value);
			}

		}
		return data;
	}

	public static Integer getSize() {

		String[] list = outputString.split("\005");
		int size = list.length;
		return size;
	}
}