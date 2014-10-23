package com.autohome.pvcount.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicBrokersReader;

public class RecUtils {

    public static final Logger LOG = LoggerFactory.getLogger(RecUtils.class);
	/**
	 * 恢复Redis中数据, 重置ZK数据
	 * @param spoutId
	 * @param zkRoot
	 */
	public static void recRedis(String spoutId, String zkRoot){
		LOG.info("reset zookeeper data ............");
		new RestZKData(spoutId,zkRoot).resetOffsetForPartition();;
		LOG.info("recory redis data ............");
		RedisUtils.recRedisData();
	}
	
	public static void main(String[] args) {
		RecUtils.recRedis("double_spout3", "10.168.100.182:2181,10.168.100.183:2181,10.168.100.184:2181");
	}
}
