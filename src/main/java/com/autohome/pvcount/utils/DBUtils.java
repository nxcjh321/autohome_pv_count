package com.autohome.pvcount.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;



public class DBUtils {
	
	public static void updatePV(){
		Connection con = null;
		Statement stmt = null;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date curDate = new Date(System.currentTimeMillis());
		String str = formatter.format(curDate);
		String date = LogUtils.dateFormat(str);
		String count = null;
		String sql = null;
		String logtype=null;
		Map<String,String> offsetdata = null;
		try {
		con = JDBCUtils.getConnection();
		stmt = con.createStatement();
		offsetdata = RedisUtils.getPOffset();
		con.setTransactionIsolation(Connection. TRANSACTION_REPEATABLE_READ);
		con.setAutoCommit(false);  
		//更新pv, 同时在线人数
		for(int i=0; i<12; i++){
			date = DateUtils.addDateMinut(date, -5*(i+1));
			count =	RedisUtils.getShowData("PC", date);
			if(count != null ){
				stmt.executeUpdate(getUpdateSql(date,"PC",count));
			}
			count = RedisUtils.getShowData("M", date);
			if(count != null ){
				stmt.executeUpdate(getUpdateSql(date,"M",count));
			}
			count = RedisUtils.getKeyCount()+"";
			if(count != null){
				stmt.executeUpdate(getUpdateSeSql(date,count));
			}
			
		}
		//更新offset
		for(Map.Entry<String, String> entry : offsetdata.entrySet()){
			stmt.executeUpdate(getUpdateOffset(entry.getKey()+"",entry.getValue()));
		}
		con.commit();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//更新PV
	public static String getUpdateSql(String date,String logtype,String count){
		
			StringBuilder builder = new StringBuilder();
			builder.append("insert into pv_count set log_time='");
			builder.append(date);
			builder.append("', log_type='");
			builder.append(logtype);
			builder.append("', pv_count='");
			builder.append(count);
			builder.append("', insert_time= NOW() ON DUPLICATE KEY UPDATE pv_count=pv_count+'");
			builder.append(count).append("';");
			return builder.toString();
		
	}
	
	//更新同时在线人数
	public static String getUpdateSeSql(String date, String count){
		//2014-10-21 15:40:00','7023752', NOW()) ON DUPLICATE KEY UPDATE se_count=se_count+'7023752';
		StringBuilder builder = new StringBuilder("insert into se_count set log_time='");
		builder.append(date);
		builder.append("', se_count='");
		builder.append(count);
		builder.append("', insert_time=NOW() ON DUPLICATE KEY UPDATE se_count=se_count+'");
		builder.append(count).append("';");
		return builder.toString();
	}
	
	
	//更新Offset
	public static String getUpdateOffset(String partition,String offset){
		
		
		StringBuilder builder = new StringBuilder();
		builder.append("update t_offset set  offset='"); 
		builder.append(offset);
		builder.append("',insert_time=NOW()");
		builder.append(" where partition_id='");
		builder.append(partition).append("';");
		return builder.toString();
	}
	
	public static void main(String[] args) {
		updatePV();
	}
	
}
