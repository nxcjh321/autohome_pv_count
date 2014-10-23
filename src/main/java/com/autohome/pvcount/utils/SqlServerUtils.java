package com.autohome.pvcount.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.autohome.pvcount.bean.OnlineCount;
import com.autohome.pvcount.bean.PVCount;

public class SqlServerUtils {
	
	public static final Logger LOG = LoggerFactory.getLogger(SqlServerUtils.class);
	static Connection con = null;
	public static Connection getConnection(){
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String url = "jdbc:sqlserver://10.168.0.49:1433;DatabaseName=pv_autohome";
			con = DriverManager.getConnection(url,"sa","shuang");
			return con;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {

			e.printStackTrace();
		} 
		return con;
	}
	
	/**
	 * 从redis中更新db数据
	 */
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
		con = SqlServerPool.getConnection();
		stmt = con.createStatement();
		offsetdata = RedisUtils.getPOffset();
//		con.setTransactionIsolation(Connection. TRANSACTION_REPEATABLE_READ);
//		con.setAutoCommit(false);  
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
		//con.commit();
		LOG.info(date+":  insert into DB.. ");
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			SqlServerPool.release(stmt, con);
		}
	}
	
	
		/**
		 * 更新PV SQL
		 * @param date
		 * @param logtype
		 * @param count
		 * @return
		 */
		public static String getUpdateSql(String date,String logtype,String count){
			StringBuilder builder = new StringBuilder();
			builder.append("if exists(select log_type from pv_count where log_type='");
			builder.append(logtype).append("' and log_time='");
			builder.append(date).append("')    ");
			builder.append("update pv_count set pv_count=pv_count+");
			builder.append(count);
			builder.append(" where log_type='");
			builder.append(logtype);
			builder.append("' and log_time='");
			builder.append(date).append("';");
			builder.append("	else	");
			builder.append("insert into pv_count (log_time, log_type, pv_count, insert_time) values ('");
			builder.append(date).append("','");
			builder.append(logtype).append("','");
			builder.append(count).append("',getdate());");
			return builder.toString();
		
		}
	
		/**
		 * 更新同时在线人数SQL
		 * @param date
		 * @param count
		 * @return
		 */
		public static String getUpdateSeSql(String date, String count){
			StringBuilder builder = new StringBuilder("if exists (select log_time from se_count where log_time='");
			builder.append(date).append("')");
			builder.append("update se_count set se_count=se_count+");
			builder.append(count);
			builder.append(" where log_time='").append(date).append("';");
			builder.append("	else	");
			builder.append("insert into se_count (log_time, se_count, insert_time) values ( '");
			builder.append(date).append("', '");
			builder.append(count);
			builder.append("', getdate());");
			return builder.toString();
		}
	
	
		/**
		 * 更新Offset  SQL
		 * @param partition
		 * @param offset
		 * @return
		 */
		public static String getUpdateOffset(String partition,String offset){
			StringBuilder builder = new StringBuilder();
			builder.append("update t_offset set  offset='"); 
			builder.append(offset);
			builder.append("',insert_time=getdate()");
			builder.append(" where partition_id='");
			builder.append(partition).append("';");
			return builder.toString();
		}
	
	
		/**
		 * 初始化Offset
		 * @param partitionNum
		 */
		public static void initOffset(int partitionNum){
			Connection con = null;
			Statement stmt = null;
			StringBuilder builder = null;
			try {
				con = SqlServerPool.getConnection();
				stmt = con.createStatement();
				for(int i=0; i<partitionNum; i++){
					builder =new StringBuilder("if not exists (select* from t_offset) insert into t_offset (partition_id, offset, insert_time) values ('");
					builder.append(i).append("','0',getdate());");
					stmt.executeUpdate(builder.toString());
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				SqlServerPool.release(stmt, con);
			}		
		}
	
		public static void selectTab() throws SQLException{
			Connection con = null;
			Statement stmt = null;
			con = SqlServerPool.getConnection();
			stmt = con.createStatement();
			String sql = "select * from pv_count;";
			ResultSet result = stmt.executeQuery(sql);
			if(result.next()){
				System.out.println(result.getString(2)+"::"+result.getString(1));
			}
			SqlServerPool.release(result, stmt, con);
		}
		
		/**
		 * 从数据库获取PV数据
		 * @param date
		 * @return
		 */
		public static Set<PVCount> getPVData(){
			Connection con = null;
			Statement stmt = null;
			ResultSet result = null;
			Set<PVCount> set = new HashSet<PVCount>();
			try {
				con = SqlServerPool.getConnection();
				stmt = con.createStatement();
				String sql = "select log_time, log_type, pv_count from pv_count where CONVERT(varchar(10), log_time, 120) = CONVERT(varchar(10), getdate(), 120); ;";
				result = stmt.executeQuery(sql);
				while(result.next()){
					set.add(new PVCount(result.getString(1),result.getString(2),result.getString(3)));
				}
				return set;
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				SqlServerPool.release(result, stmt, con);
			}
			return null;
		}
	
	
		/**
		 * 从数据库获得OnlineData
		 * @param date
		 * @return
		 */
		public static Set<OnlineCount> getOnlineData(){
			Connection con = null;
			Statement stmt = null;
			ResultSet result = null;
			Set<OnlineCount> set = new HashSet<OnlineCount>();
			try {
				con = SqlServerPool.getConnection();
				stmt = con.createStatement();
				String sql = "select log_time, se_count from se_count where CONVERT(varchar(10), log_time, 120) = CONVERT(varchar(10), getdate(), 120);;";
				result = stmt.executeQuery(sql);
				while(result.next()){
					set.add(new OnlineCount(result.getString(1),result.getString(2)));
				}
				return set;
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				SqlServerPool.release(result, stmt, con);
			}
			return null;
		}
	
		public static Map<String,String> getPartitionAndOffset(){
			Connection con = null;
			Statement stmt = null;
			ResultSet result = null;
			Map<String,String> map = new HashMap<String,String>();
			try {
				con = SqlServerPool.getConnection();
				stmt = con.createStatement();
				String sql = "select partition_id, offset from t_offset ;";
				result = stmt.executeQuery(sql);
				while(result.next()){
					map.put(result.getString(1), result.getString(2));
				}
				return map;
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				SqlServerPool.release(result, stmt, con);
			}
			return null;
		}
	
		public static void main(String[] args) throws SQLException {
			//getPartitionAndOffset();
			//getOnlineData("2014-10-22");
			//updatePV();
		}
}
