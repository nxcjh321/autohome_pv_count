package com.autohome.pvcount.utils;

import java.text.ParseException;

import backtype.storm.utils.Time;





public class ThreadUpdateUtils {
	private static ThreadS _runner;
	
	public static void updateMysql(int ternalTime){
		final long expirationMillis = ternalTime*1000;
		_runner = new ThreadS(new Runnable() {
			
			public void run() {
				try{
					Thread.sleep(1000*64*3);
					while(true){
						Time.sleep(expirationMillis);
						 synchronized(this) {
							 SqlServerUtils.updatePV();
						 }
					}
				} catch (InterruptedException ex) {
					ex.printStackTrace();
                }
			}
		});
		_runner.setDaemon(true);
		_runner.start();
	}
	
	public static void main(String[] args) {
		
		new Thread(new Runnable() {
		
			public void run() {
				// TODO Auto-generated method stub
					try {
						updateMysql(10);
						Thread.sleep(999999999);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}).start();;
	}
}
