package Refactoring.Common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;

public class tools {

	public static long strToLong(String ipStr){
		String[] datas = ipStr.split("\\.");
		long ipLong=0;
		for (int a=0;a<datas.length;a++){
			ipLong = ipLong*256 + Integer.valueOf(datas[a]);
		}
		return ipLong;
	}
	
	public static String join(ArrayList<String> array, String joinType){
		String resultString = "";
		if (array.size() == 0){
			return resultString;
		}
		Iterator<String> it=array.iterator();
		resultString = it.next();
		while(it.hasNext()){
		    
		    resultString  += joinType +  it.next();
		    //System.out.println(">>>resultString: " + resultString);
		}
		return resultString;
	}
	
	public static Map getData(String jsonString){
		Map<String, String> result = new HashMap<String, String>();
		for (String data:jsonString.split("&")){
			String[] dList = data.split("=");
			if (dList.length==1){
				result.put(dList[0], "");
			}
			if (dList.length==2){
				result.put(dList[0], dList[1]);
			}
			
		}
		return result;
	}
	
	public static ArrayList getList(ArrayList stringList, Map<String, String> dataMap, LinkedHashSet <String> Set, String timeString, String ipString){
		stringList.add(timeString);
		stringList.add(ipString);
		for(String set:Set){
			//System.out.println("SET: " + set);
			if (dataMap.containsKey(set)){
				if (set.equals("CLE")){
					System.out.println("CLE");
					stringList.add(dataMap.get(set));
				}else{
					String value = java.net.URLDecoder.decode(dataMap.get(set).trim());
					String valuede = java.net.URLDecoder.decode(value.trim());
					String valuede2 = java.net.URLDecoder.decode(valuede.trim());
					//System.out.println("value decode :" + valuede2);
					//String va = set+ "=" + valuede2;
					//System.out.println(set + " :" + valuede2.trim());
					stringList.add(valuede2.trim());
				}
			}else{
				stringList.add(" ");
			}
		}
		return stringList;
		
	}
	
	public static Map sortMap(Map oldMap) {  
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(oldMap.entrySet());  
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {  
  
            public int compare1(Entry<java.lang.String, Integer> arg0,  
                    Entry<java.lang.String, Integer> arg1) {  
                return arg0.getValue() - arg1.getValue();  
            }

			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				// TODO Auto-generated method stub
				return 0;
			}  
        });  
        Map newMap = new LinkedHashMap();  
        for (int i = 0; i < list.size(); i++) {  
            newMap.put(list.get(i).getKey(), list.get(i).getValue());  
        }  
        return newMap;  
    }  
	
	/* 时间戳转换成日期格式字符串 
    * @param seconds 精确到秒的字符串 
    * @param formatStr 
    * @return 
    */  
   public static String timeStamp2Date(String seconds,String format) {  
       if(seconds == null || seconds.isEmpty() || seconds.equals("null")){  
           return "";  
       }  
       if(format == null || format.isEmpty()) format = "yyyy-MM-dd HH:mm:ss";  
       SimpleDateFormat sdf = new SimpleDateFormat(format);  
       return sdf.format(new Date(Long.valueOf(seconds+"000")));  
   }  
   
   /** 
    * 日期格式字符串转换成时间戳 
    * @param date 字符串日期 
    * @param format 如：yyyy-MM-dd HH:mm:ss 
    * @return 
    */  
   public static String date2TimeStamp(String date_str,String format){  
       try {  
           SimpleDateFormat sdf = new SimpleDateFormat(format);  
           return String.valueOf(sdf.parse(date_str).getTime()/1000);  
       } catch (Exception e) {  
           e.printStackTrace();  
       }  
       return "";  
   }
	
}