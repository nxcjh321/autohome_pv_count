package com.autohome.pvcount.utils;

public class LogUtils {

	/**
	 * 把2014-10-10 19:02:31.511 转化成2014-10-10 19:05
	 * @param logDate
	 * @return
	 */
	public static String dateFormat(String logDate){
		
		String date = logDate.substring(0, 16);
		String hour = logDate.substring(0, 16).substring(11, 13);
		String min = logDate.substring(0, 16).substring(14, 16);
		
		if(hour.startsWith("0")){
			hour = hour.substring(1);
		}else{
			
		}
		
		if(min.startsWith("0")){
			min = min.substring(1);
		}else{
		}
		 int _min = Integer.parseInt(min);
		 int _hour =  Integer.parseInt(hour);
		 int cha = _min%10;
		 
		 if(cha <= 5){
			 _min = _min + (5-cha);
			 if(_min == 5){
		    		min = "0"+_min;
		    	}else{
		    		min = _min+"";
		    	}
			 
		    }else if(cha >5){
		    	_min = _min + (10 -cha);
		    	if(_min == 60){
		    		_hour++;
		    		min = "00";
		    	}else{
		    		min = _min+"";
		    	}
		    }

		 if(_hour < 10){
			 hour = "0" + _hour;
		 }else{
			 hour = _hour+"";
		 }
		 String time = hour+":"+min;
		 date = date.substring(0, 11)+time+":00";
		 System.out.println(date.substring(0, 11)+time);
		 System.out.println("======"+date);
		 return date;
	}
	
	public static void main(String[] args) {
		dateFormat("2014-10-10 08:00:31.511");
	}
}
