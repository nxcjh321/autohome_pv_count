package com.autohome.pvcount.utils;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestZKData {
	
	public static final Logger LOG = LoggerFactory.getLogger(RestZKData.class);
	private String clientId;
	private  CuratorFramework _client;
	
	public RestZKData(String clientId,String zkRoot){
		this.clientId = clientId;
		
		 _client = CuratorFrameworkFactory.builder().connectString(zkRoot)
				.namespace("transactional").retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
				.connectionTimeoutMs(5000).build();
		_client.start();
	}
	
	/**
	 * 获取parition个数
	 * @return
	 */
	public int getNumPartitions() {
			String topicBrokersPath = "/"+clientId+ "/user";
			List<String> children;
			try {
				children = _client.getChildren().forPath(topicBrokersPath);
				System.out.println(children.size());
				return children.size();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return 0;
	}
	
	/**
	 * 重置partition 中的offset
	 */
	public void resetOffsetForPartition(){
		Map<String,String> map = SqlServerUtils.getPartitionAndOffset();
		int num = getNumPartitions();
			try {
				for(int i=0; i<num; i++){
					String value = map.get(i+"");
					String path = "/double_spout3/user/partition_"+i;
				    List<String>children = _client.getChildren().forPath(path);
				    
					for(int j=0; j<children.size(); j++){
						path = path+"/"+children.get(j);
						updateKafkaOffsetData( path, 0+"");
					}
				}
				_client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}	
	}
	
	
	/**
	 * 更新node数据
	 * @param path
	 * @param value
	 * @throws Exception
	 */
	public  void updateKafkaOffsetData( String path, String value) throws Exception{
		byte[] data = _client.getData().watched().forPath(path);
		String str = new String(data);
		_client.setData().forPath(path, setJsonValue(str, value).getBytes("UTF8"));
		LOG.info("update zookeeper node for path: "+path+ ", value :"+ value);
	}
	
	/**
	 * 修改node中的json串
	 * @param jsonStr
	 * @param offsetValue
	 * @return
	 */
	public  String setJsonValue(String jsonStr,String offsetValue){
		 Object obj =  JSONValue.parse(jsonStr);
		 JSONObject jsonObj =   (JSONObject)obj;
		 if(jsonObj.containsKey("nextOffset")){
			 jsonObj.put("nextOffset", offsetValue);
			 jsonObj.put("offset", offsetValue);
		 }
		 return jsonObj.toJSONString();
	}
	
	public static void main(String[] args) throws Exception {
		new RestZKData("double_spout3","10.168.100.182:2181,10.168.100.183:2181,10.168.100.184:2181").resetOffsetForPartition();
	}
	
	
	

}
