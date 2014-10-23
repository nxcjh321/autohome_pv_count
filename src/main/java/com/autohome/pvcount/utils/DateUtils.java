package com.autohome.pvcount.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	
	public static String addDateMinut(String day, int x) throws ParseException{
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		date = format.parse(day);
		if(date == null){
			
			return "";
		}else{
			System.out.println("format :"+format.format(date));
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.MINUTE, x);
			date = cal.getTime();
			System.out.println("afer: "+format.format(date));
			cal = null;
			return format.format(date);
		}
	}
	
	public static void main(String[] args) throws ParseException {
		System.out.println(addDateMinut("2014-11-10 08:06:30",-5));
	}
	
}
