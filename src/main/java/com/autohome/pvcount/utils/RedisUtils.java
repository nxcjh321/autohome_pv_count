package com.autohome.pvcount.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.autohome.pvcount.bean.OnlineCount;
import com.autohome.pvcount.bean.PVCount;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisUtils extends Observable{


	public static final Logger LOG = LoggerFactory.getLogger(RedisUtils.class);
	private static JedisPool pool;
	
	public static Lock lock = new ReentrantLock();
	public static void initPool(){
		
		ResourceBundle bundle = ResourceBundle.getBundle("layout");
		if(bundle == null){
			throw new IllegalArgumentException("[redis.properties] is not found");
		}
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
		config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
		config.setMaxWait(Long.valueOf(bundle.getString("redis.pool.maxWait")));
		config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
		config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));
		pool = new JedisPool(config,bundle.getString("redis.ip"), 6379,100000);
	}
	
	public static Jedis getJedis(){
		if(pool == null)
			initPool();
		    Jedis jedis = pool.getResource();
			return jedis;
	}
	
	public static void returnJedis(Jedis jedis){
		if(jedis != null){
			pool.returnBrokenResource(jedis);
		}		
	}

	
/*****************************************************************************************/
/*******************************   Redis  ************************************************/
/*****************************************************************************************/


	/**
	 * 查询redis中 uv是否存在,若不存在, 进行保存, 并返回结果.
	 * @param UVid
	 * @return
	 */
	public static boolean insertUV(String UVid){
		Jedis jedis = RedisUtils.getJedis();
		
		if(jedis.exists(UVid)){
			jedis.expire(UVid, 60*30);
			RedisUtils.returnJedis(jedis);
			return false;
		}else{
			jedis.hset(UVid,"1", "1");
			jedis.expire(UVid, 60*30);
			RedisUtils.returnJedis(jedis);
			return true;
		}	
	
	}
	
	
	
	/**
	 * 获取type, date对应的pv量
	 * @param logtype
	 * @param date
	 * @return
	 */
	public static String getShowData(String logtype, String date){
		Jedis jedis = getJedis();
		String count = jedis.hget(logtype, date);
		RedisUtils.returnJedis(jedis);
		return count;
	}
	

	
	/**
	 * 获取type, date对应的pv量
	 * @param type
	 * @param date
	 * @return
	 * @throws ParseException
	 */
	public static String getPVData(String type, String date) throws ParseException{
		Jedis jedis = getJedis();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date curDate = new Date(System.currentTimeMillis());
		String str = formatter.format(curDate);
		date = LogUtils.dateFormat(str);
		RedisUtils.returnJedis(jedis);
		return jedis.hget(type, date);
	}
	
	/**
	 * 获取redis中同时在线人数
	 * @return
	 */
	public static long getKeyCount(){
		Jedis jedis = getJedis();
		long size = jedis.dbSize();
		RedisUtils.returnJedis(jedis);
		return size;
	}
	
	 /**
	  * 随机获取36个字符的的字符串
	  * @param length
	  * @return
	  */
	 public static String getCharAndNumr(Integer length) { 
	        StringBuffer sb = new StringBuffer(); 
	        Random random = new Random(); 
	        for (int i = 0; i < length; i++) { 
	            String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num"; // 输出字母还是数字 
	            if ("char".equalsIgnoreCase(charOrNum)){// 字符串 
	                int choice = random.nextInt(2) % 2 == 0 ? 65 : 97; // 取得大写字母还是小写字母 
	                sb.append((char) (choice + random.nextInt(26))); 
	            } else if ("num".equalsIgnoreCase(charOrNum)){// 数字 
	                sb.append(String.valueOf(random.nextInt(10))); 
	            } 
	        } 
	        return sb.toString(); 
	    } 

	/**
	 * 存储partition, offset, txid信息
	 * @param txid
	 * @param partitionId
	 * @param offset
	 */
	 public static void saveOffset(String partitionId, String txid, String offset){
		 Jedis jedis = getJedis();
		 jedis.hset("partition_"+partitionId,txid, offset);
		 RedisUtils.returnJedis(jedis);
	 }
	 
	 /**
	  * 获取Partition, offset
	  * @param partitionNum
	  * @param txid
	  * @return
	  */
	 public static String[][] getOffset(int partitionNum,String txid){
		 Jedis jedis = getJedis();
		String offset; 
		String[][] data = new String[3][3];
		 for(int i=0; i<partitionNum; i++){
			offset = jedis.hget("partition_"+i, txid);
			 if( offset != null){
				 data[i] =  new String[]{i+"",offset,txid};
			 }
		 }
		 RedisUtils.returnJedis(jedis);	 
		 return data;
	 }
	 
	 
	 
		public void testsitde(){
			Jedis jedis = getJedis();
			System.out.println(jedis.hget("3D0x8hbI1E57QL85MbFR66i11488D2ghwB0k", "1"));
			System.out.println(jedis.hget("2", ""));;
			jedis.del("0");jedis.del("1");jedis.del("2");jedis.del("3");
			Map<String,String> map = jedis.hgetAll("partition_offset");	
			RedisUtils.returnJedis(jedis);
		}
	 
	 
	 /**
	  * 获取offset
	  * @return
	  */
	 public static Map<String,String> getPOffset(){
		 Jedis jedis = getJedis();
		 String[] data = new String[3];
		 Map<String,String> map = jedis.hgetAll("partition_offset");
		 RedisUtils.returnJedis(jedis);	
		 return map;
	 }
	 

	 public static void saveTxidS(String txid){
		 Jedis jedis = getJedis();
		jedis.hset("Txids", "txid", txid);
		 RedisUtils.returnJedis(jedis);	
	 }
	 
	 public static String getTxidS(String txid){
		 Jedis jedis = getJedis();
		String tx = jedis.hget("Txids", "txid");
		 RedisUtils.returnJedis(jedis);	
		 return tx;
	 }

	 /**
		 * 恢复Redis中数据
		 */
		public static void recRedisData(){
			Jedis jedis = getJedis();
			OnlineCount online = new OnlineCount();
			PVCount pv = new PVCount();
			Set<OnlineCount> online_set = SqlServerUtils.getOnlineData();
			Set<PVCount> pv_set =SqlServerUtils.getPVData();
			Map<String,String> offset_map = SqlServerUtils.getPartitionAndOffset();
			Iterator it = online_set.iterator();
			while(it.hasNext()){
				online = (OnlineCount) it.next();
				jedis.hset("UV",online.getLog_time(), online.getSe_count());
				LOG.info("recory redis for online data , date: "+online.getLog_time() +", count:"+online.getSe_count());
			}
			it = pv_set.iterator();
			while(it.hasNext()){
				pv = (PVCount) it.next();
				jedis.hset(pv.getLog_type(),pv.getLog_time(),pv.getPv_count());
				LOG.info("recory redis for PV , type:"+ pv.getLog_type()+", date: "+pv.getLog_time() +", count:"+pv.getPv_count());
			}
			for(Map.Entry<String, String> entry : offset_map.entrySet()){
				jedis.hset("partition_offset", entry.getKey(), entry.getValue());
				LOG.info("recory redis for Offset partition:"+ entry.getKey()+", value: "+entry.getValue());
			}
			RedisUtils.returnJedis(jedis);
		}
	 
	 /**************************************************************************************************************************/
	 /*******************************************      TEST     ****************************************************************/
	 /**************************************************************************************************************************/
	  
	 
	 	
		public void testInsert() throws InterruptedException{
			
			final Jedis jedis = getJedis();
			String date = getCharAndNumr(36);
			for(int i=0; i<10000000; i++){
				date = getCharAndNumr(36);
				if(jedis.exists(date)){
					jedis.expire(date, 60*30);
				}else{
					jedis.hset(date,"1", "1");
					jedis.expire(date, 60*30);
				}	
				System.out.println(i+"----"+date);
			}
			RedisUtils.returnJedis(jedis);
		}
	 
		
		public void testsite(){
			Jedis jedis = getJedis();
			System.out.println(jedis.hget("3D0x8hbI1E57QL85MbFR66i11488D2ghwB0k", "1"));
			System.out.println(jedis.hget("1", ""));;
			RedisUtils.returnJedis(jedis);
		}
	 
		
	 
	public static void main(String[] args) throws InterruptedException {
		
		final Jedis jedis = getJedis();
		Map<String, String> str = jedis.hgetAll("PC");
		for(Map.Entry<String, String> entry : str.entrySet()){
			System.out.println("PC====="+entry.getKey()+"::::"+entry.getValue());
		}
		Map<String, String> str1 = jedis.hgetAll("M");
		for(Map.Entry<String, String> entry : str1.entrySet()){
			System.out.println("M====="+entry.getKey()+"::::"+entry.getValue());
		}
		Map<String, String> str2 = jedis.hgetAll("UV");
		for(Map.Entry<String, String> entry : str2.entrySet()){
			System.out.println("UV====="+entry.getKey()+"::::"+entry.getValue());
		}
		
		for(int j=0; j<3; j++){
			System.out.println(jedis.hget("partition_offset", j+""));;
		}
		
//		//System.out.println(str);
		Map<String,String> map = jedis.hgetAll("partition_0");
		Map<String,String> map1 = jedis.hgetAll("partition_1");
		Map<String,String> map2 = jedis.hgetAll("partition_2");
		System.out.println(map.toString());
		System.out.println(map1.toString());
		System.out.println(map2.toString());
		
		Map<String,String> map3 =jedis.hgetAll("partition_offset");
		System.out.println(map3.toString());
		
		//recRedisData();
		 
		 System.out.println(getPOffset());
		//jedis.del("partition_offset");jedis.del("PC");jedis.del("M");jedis.del("UV");jedis.del("0");jedis.del("1");jedis.del("2");jedis.del("partition_1");jedis.del("partition_2");jedis.del("partition_0");
		

		
		
	
		RedisUtils.returnJedis(jedis);
	}

}

