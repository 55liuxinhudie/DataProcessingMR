package Refactoring.Common;

import java.util.Map;
import java.util.HashMap;



public class getFromData {

	public static Map getData(String jsonString){
		Map<String, String> result = new HashMap<String, String>();
		for (String data:jsonString.split("&")){
			String[] dList = data.split("=");
			if (dList.length==1){
				result.put(dList[0], " ");
			}
			if (dList.length==2){
				result.put(dList[0], dList[1]);
			}
			
		}
		return result;
	}
}
