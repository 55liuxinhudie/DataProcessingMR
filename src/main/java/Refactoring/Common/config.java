package Refactoring.Common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import Refactoring.Common.tools;

public class config {
    private static final Map<String, Integer> configMap = new HashMap<String, Integer>();
    
    public void init(String configFile){
    	Properties props = new Properties();
    	InputStream in = null;
    	try{
    		in = new config().getClass().getResourceAsStream("/"+configFile);
    		props.load(in);
    		for(Entry<Object, Object> e: props.entrySet()){
    			String value = ((String) e.getValue()).trim();
    			Integer it = new Integer(value);
    			int i = it.intValue();  
    			//System.out.println(((String) e.getKey()).trim() + "= " + i);
    			configMap.put(((String) e.getKey()).trim(), i);
    		}
    		in.close();
    		props = null;
    	} catch (FileNotFoundException e){
    		e.printStackTrace();
    	} catch (IOException e1) {
            System.out.println("read config file failed :" + configFile);
            e1.printStackTrace();
            throw new RuntimeException("read config file failed :" + configFile);
    	} finally{
    		try{
    			if (in != null){
    				in.close();
    			}
    		} catch (IOException e){
    			e.printStackTrace();
    		}
    	}
    }
    
    public static Integer getValue(String key){
    	return configMap.get(key);
    }
    
    public static Set<String> getKeys(){
    	return configMap.keySet();
    }
    
    public static Map<String, Integer> getConfigMap(){
    	return configMap;
    }
    public static Map<String, Integer> getSortMap(){
    	Map sortConfig = tools.sortMap(configMap);
    	return sortConfig;
    }
    
    public static LinkedHashSet <String> getKeySet(){
    	//System.out.println("config size: " + config.size());
    	Map map = tools.sortMap(configMap);
    	List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>(map.entrySet());  
        Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {  
            //升序排序  
            public int compare(Entry<String, Integer> o1,  
                    Entry<String, Integer> o2) {  
                return o1.getValue().compareTo(o2.getValue());  
            }  
  
        });  
  
        LinkedHashSet <String> Set = new LinkedHashSet <String>();
        
        for(Map.Entry<String,Integer> mapping:list){   
               //System.out.println(mapping.getKey()+":"+mapping.getValue());   
               Set.add(mapping.getKey());
        }    
        configMap.clear();
        return Set;
    }
    
}