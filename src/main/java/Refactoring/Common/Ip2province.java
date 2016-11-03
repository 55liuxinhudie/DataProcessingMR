package Refactoring.Common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class Ip2province {
	public static HashMap<String, String> IP2province(String ip) {
		String url_phone = "http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=js&ip=" + ip;
		String url_return = null;
		HashMap province_country = new HashMap();
		HttpURLConnection url_con = null;
		try {
			URL url = new URL(url_phone);
			StringBuffer bankXmlBuffer = new StringBuffer();

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setDoOutput(true);
			connection.setRequestProperty("User-Agent", "directclient");
			PrintWriter out = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "utf-8"));

			out.close();
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				//System.out.print("inputLine: " + inputLine);
				bankXmlBuffer.append(inputLine);
			}
			in.close();
			url_return = bankXmlBuffer.toString();
		} catch (Exception e) {
			System.out.println("发送GET请求出现异常！" + e);
			e.printStackTrace();
		} finally {
			if (url_con != null) {
				url_con.disconnect();
			}
		}
		String[] json = url_return.split("=");
		//String js = "{\"ret\":1,\"start\":-1,\"end\":-1,\"country\":\"\u4e2d\u56fd\",\"province\":\"\u5317\u4eac\",\"city\":\"\u5317\u4eac\",\"district\":\"\",\"isp\":\"\",\"type\":\"\",\"desc\":\"\"}";
		String js = json[1].trim().split(";")[0];
		
		HashMap<String, String> jsmap = new HashMap<String, String>();
		jsmap = toHashMap(js.trim());
		String province = jsmap.get("province");
		String country = jsmap.get("country");
		String city = jsmap.get("city");
		province_country.put("province", province);
		province_country.put("country", country);
		province_country.put("city", city);
		
		return province_country;
	}

	private static HashMap<String, String> toHashMap(Object object)  
	   {  
	       HashMap<String, String> data = new HashMap<String, String>();  
	       // 将json字符串转换成jsonObject  
	       JSONObject jsonObject = JSONObject.fromObject(object);  
	       //System.out.print("print" + object);
	       Iterator it = jsonObject.keys();  
	       // 遍历jsonObject数据，添加到Map对象  
	       while (it.hasNext())  
	       {  
	           String key = String.valueOf(it.next());  
	           if (key.equals("country") || key.equals("province") || key.equals("city")){
	        	   String value = (String) jsonObject.get(key);  
		           data.put(key, value);  
	           }
	           
	       }  
	       return data;  
	   }  
}