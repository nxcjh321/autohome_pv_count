package com.autohome.pvcount.bean;

public class PVCount {

	private String log_time;
	private String log_type;
	private String pv_count;
	private String insert_time;
	public String getLog_time() {
		return log_time;
	}
	public void setLog_time(String log_time) {
		this.log_time = log_time;
	}
	public String getLog_type() {
		return log_type;
	}
	public void setLog_type(String log_type) {
		this.log_type = log_type;
	}
	public String getPv_count() {
		return pv_count;
	}
	public void setPv_count(String pv_count) {
		this.pv_count = pv_count;
	}
	public String getInsert_time() {
		return insert_time;
	}
	public void setInsert_time(String insert_time) {
		this.insert_time = insert_time;
	}
	public PVCount(String log_time, String log_type, String pv_count) {
		super();
		this.log_time = log_time;
		this.log_type = log_type;
		this.pv_count = pv_count;
	}
	public PVCount() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
}
