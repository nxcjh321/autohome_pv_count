package com.autohome.pvcount.utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@SuppressWarnings("all")
public class Conf {
	private static Properties pps = null;
	private static Map<String, String[]> fieldFormats;
	static {
		InputStream in = Conf.class.getClassLoader().getResourceAsStream("layout.properties");
		pps = new Properties();
		try {
			pps.load(in);
			fieldFormats = new HashMap<String,String[]>();
			Set keys = pps.keySet();
			String k = null;
			String[] fieldNames = null;
			
			for(Object key : keys) {
				k = key.toString();
				fieldNames = pps.get(k).toString().split(",");
				fieldFormats.put(k, fieldNames);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String[] getLogFormat(String logtype) {
		return fieldFormats.get(logtype);
	}
	
	public static String getProperty(String key) {
		return pps.getProperty(key);
		
	}

	public static void main(String[] args) {
	}
}