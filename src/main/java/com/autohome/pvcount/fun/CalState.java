package com.autohome.pvcount.fun;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





import com.autohome.pvcount.utils.RedisUtils;

import redis.clients.jedis.Jedis;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
public class CalState implements IBackingMap<TransactionalValue<Long>>{
	
	public static final Logger LOG = LoggerFactory.getLogger(CalState.class);
	public Jedis jedis = null;
	CalState(Jedis jedis){
		this.jedis = jedis;
	}
	@Override
	public List<TransactionalValue<Long>> multiGet(List<List<Object>> keys) {
		// TODO Auto-generated method stub
		List result = new ArrayList();
		for(List<Object> key : keys) {
			String date = key.get(0).toString();
			String logtype = key.get(1).toString();
			String pv = jedis.hget(date, logtype);
			if(pv==null){
				result.add(new TransactionalValue(0l, 0l));
			}else{
				String txid = jedis.hget(date, "t-"+logtype);
				result.add(new TransactionalValue(Long.parseLong(txid), Long.parseLong(pv)));
			}
			result.add(new TransactionalValue(0l, 0l));
		}
		return result;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<TransactionalValue<Long>> vals) {
		
		int num = Math.min(keys.size(), vals.size());
		List _key = null;
		TransactionalValue<Long> _val=null;
		Long txid = vals.get(0).getTxid();
		for(int i=0;i<num;i++){
			_key = keys.get(i);
			_val=vals.get(i);
			
			jedis.hset(_key.get(0).toString(), _key.get(1).toString(), _val.getVal()+"");
			jedis.hset(_key.get(0).toString(), "t-"+_key.get(1).toString(), _val.getTxid()+"");
		}
		//存储txid
		String[][] par_offset = RedisUtils.getOffset(3,txid+"");
		if(par_offset == null){
			System.out.println(par_offset);
		}
		for(int i=0; i<3; i++){
			if(par_offset[i][0] != null && par_offset[i][1] != null){
				jedis.hset("partition_offset",par_offset[i][0],par_offset[i][1]);
			}
			
		}	
	}
	
}
