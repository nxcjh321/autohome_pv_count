package com.autohome.pvcount.bean;

public class OnlineCount {

	private String log_time;
	private String se_count;
	public String getLog_time() {
		return log_time;
	}
	public void setLog_time(String log_time) {
		this.log_time = log_time;
	}
	public String getSe_count() {
		return se_count;
	}
	public void setSe_count(String se_count) {
		this.se_count = se_count;
	}
	public OnlineCount(String log_time, String se_count) {
		super();
		this.log_time = log_time;
		this.se_count = se_count;
	}
	public OnlineCount() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
}
