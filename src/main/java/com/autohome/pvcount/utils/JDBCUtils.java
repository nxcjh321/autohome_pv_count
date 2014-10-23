package com.autohome.pvcount.utils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class JDBCUtils {

	    private static ComboPooledDataSource ds;
	    static {
	      try {
	    	ds = new ComboPooledDataSource();
			ds.setDriverClass("com.mysql.jdbc.Driver");
	        ds.setJdbcUrl(Conf.getProperty("host"));
	        ds.setUser(Conf.getProperty("username"));
	        ds.setPassword(Conf.getProperty("password"));

	        //初始化时获取三个连接，取值应在minPoolSize与maxPoolSize之间。Default: 3 initialPoolSize
	        ds.setInitialPoolSize(1);
	        //连接池中保留的最大连接数。Default: 15 maxPoolSize
	        ds.setMaxPoolSize(3);
	        //// 连接池中保留的最小连接数。
	        ds.setMinPoolSize(1);
	        //当连接池中的连接耗尽的时候c3p0一次同时获取的连接数。Default: 3 acquireIncrement
	        ds.setAcquireIncrement(1);
	        //每60秒检查所有连接池中的空闲连接。Default: 0  idleConnectionTestPeriod
	        ds.setIdleConnectionTestPeriod(25000);
	        //最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0  maxIdleTime
	        ds.setMaxIdleTime(25000);
	        //连接关闭时默认将所有未提交的操作回滚。Default: false autoCommitOnClose
	        ds.setAutoCommitOnClose(true);
	        //定义在从数据库获取新连接失败后重复尝试的次数。Default: 30  acquireRetryAttempts
	        ds.setAcquireRetryAttempts(30);
	        //两次连接中间隔时间，单位毫秒。Default: 1000 acquireRetryDelay
	        ds.setAcquireRetryDelay(1000);
	        //获取连接失败将会引起所有等待连接池来获取连接的线程抛出异常。但是数据源仍有效
	        //保留，并在下次调用getConnection()的时候继续尝试获取连接。如果设为true，那么在尝试
	        //获取连接失败后该数据源将申明已断开并永久关闭。Default: false  breakAfterAcquireFailure
	        ds.setBreakAfterAcquireFailure(true);
	        } catch (PropertyVetoException e) {
				e.printStackTrace();
			}
	        
	    }
	   
		public static Connection getConnection(){
			 Connection con = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				String url = "jdbc:mysql://10.168.100.182:3306/pvcount?useUnicode=true&characterEncoding=utf-8";
				con = DriverManager.getConnection(url,"hive","hive");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {

				e.printStackTrace();
			} 
			return con;
		}

//	    public  static Connection getConnection() {
//	        try {
//	            return ds.getConnection();
//	        } catch (SQLException e) {
//	            e.printStackTrace();
//	        }
//	        return null;
//	    }
	    
	    //释放资源.
		public static void release(Statement stmt,Connection conn){
			// 释放资源
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				stmt = null;
			}
	
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				conn = null;
			}
		}
		
		public static void release(ResultSet rs ,Statement stmt,Connection conn){
			// 释放资源
			if(rs != null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				rs = null;
			}
			release(stmt, conn);
		}
}
