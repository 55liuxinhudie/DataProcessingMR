package Refactoring.Common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashSet;

public class getFromConfig {

	public LinkedHashSet get(String fileName) throws IOException {
		File directory = new File("");//参数为空 
		String courseFile = directory.getCanonicalPath() ; 
		System.out.println("=====> " + courseFile); 
		LinkedHashSet <String> configSet = new LinkedHashSet<String>();
		String file = "src/main/resources/" + fileName;
		RandomAccessFile raf = new RandomAccessFile(new File(file),"r");
		String s ;
		
		while((s =raf.readLine())!=null){
			configSet.add(s);
			}
		raf.close();
		
		return configSet;
	}
	
	
		
}
