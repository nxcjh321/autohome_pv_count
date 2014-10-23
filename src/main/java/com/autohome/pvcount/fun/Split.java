package com.autohome.pvcount.fun;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;










import com.autohome.pvcount.utils.LogUtils;
import com.autohome.pvcount.utils.RedisUtils;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;


public class Split extends BaseFunction {
	public static final Logger LOG = LoggerFactory.getLogger(Split.class);
	private int pvisit;
	private String date;
	private String[] data;
	private String txid;
	private String offset;
	private String partition;
	private TridentOperationContext context;


	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	String line = tuple.getStringByField("str");
    	System.out.println(line);
    	String[] tt = line.split("\\\\t");
    	
    	if(tt.length == 32){
    		pvisit = Integer.parseInt(tt[2]);
    		System.out.println(tt[0]);
    		data = tt[0].trim().split("&");
    		String[] params = data[0].trim().split("@");
    		txid = params[0];
    		partition = params[1];
    		offset = params[2];
    		System.out.println(data[0]);
        	date = LogUtils.dateFormat(data[1]);
        	//存储事务ID, partition, offset
        	RedisUtils.saveOffset(partition, txid, offset);
        	System.out.println("TPO::"+txid+":::"+partition+":::"+offset);
        	//判断在线人数, 判断UV在redis中是否存在
			if(RedisUtils.insertUV(tt[30])){
				collector.emit(new Values("UV",date,txid,partition,offset));
			}
			//格式化时间格式, 并判断PC和M分别发送
        	if(tt[0].length() >= 16){
    	    	if(pvisit < 1211000){ //PC端
    	    		collector.emit(new Values("PC",date,txid,partition,offset));
    	    	}else if(pvisit >= 1211000){
    	    		collector.emit(new Values("M",date,txid,partition,offset));
    	    	}
    	    }
    	}
    	
    }
    
}