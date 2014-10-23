package com.autohome.pvcount.fun;

import java.util.HashMap;
import java.util.Map;

import org.mortbay.log.Log;


import backtype.storm.task.IMetricsContext;
import redis.clients.jedis.Jedis;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.TransactionalMap;
public class CalStateFactory implements StateFactory{

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		Jedis jedis=null;
		try {
			jedis = new Jedis("10.168.100.180",6379);
			Log.info("makeState is running ** ");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return TransactionalMap.build((IBackingMap)new CalState(jedis));
	}
//	public State makeState(Map conf, IMetricsContext metrics,
//			int partitionIndex, int numPartitions) {
//		Mongo mongo=null;
//		try {
//			mongo = new Mongo("10.168.100.181");
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		DB db = mongo.getDB("test");
//		DBCollection usertag = db.getCollection("usertag");
//		System.out.println(" mongo new  ");
//		// TODO Auto-generated method stub
//		return TransactionalMap.build((IBackingMap)new CalState(usertag));
//	}
}
