package com.autohome.pvcount.topology;


import com.autohome.pvcount.fun.CalStateFactory;
import com.autohome.pvcount.fun.Count;
import com.autohome.pvcount.fun.Split;
import com.autohome.pvcount.utils.RecUtils;
import com.autohome.pvcount.utils.RestZKData;
import com.autohome.pvcount.utils.ThreadUpdateUtils;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


import storm.kafka.trident.StormKafkaTridentSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;




public class RestDCountTopology {
	/**
	 * @param args
	 */
	
	
	   public static StormTopology buildTopology(String topologyName, String topic_id, String spout_id, String zkIp) {
		   
			TridentTopology topology = new TridentTopology();
			TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts(zkIp,"/brokers"),topic_id,topologyName);
			kafkaConfig.scheme =  new SchemeAsMultiScheme(new StringScheme());
			TridentState wordCounts = topology.newStream(spout_id, new StormKafkaTridentSpout(kafkaConfig))
					.each(new Fields("str"), new Split(),new Fields("type","date","txid","partitionid","offset")).parallelismHint(3)
					.groupBy(new Fields("type","date")).persistentAggregate(new CalStateFactory(),new Count(),new Fields("count")).parallelismHint(3);
			return topology.build();
		   
	   }
	   

	public static void main(String[] args) {

		Config conf = new Config();
		conf.setNumWorkers(4);
		conf.setNumAckers(2);
		conf.put( Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		ThreadUpdateUtils.updateMysql(60);	
		  if (args != null && args.length > 1) {
			  String topologyName = args[0];
			  String topic_id = args[1];
			  String spout_id = args[2];
			  String kafkaZookeeper = args[3];
			  RecUtils.recRedis(args[2], args[3]);
			  System.out.println("########################################");
			  System.out.println("########## Submit to Storm    ##########");
			  System.out.println("########################################");
				try {
					StormSubmitter.submitTopology(topologyName, conf, buildTopology(topologyName,topic_id,spout_id,kafkaZookeeper));
				} catch (AlreadyAliveException e) {
					e.printStackTrace();
				} catch (InvalidTopologyException e) {
					e.printStackTrace();
				}
		  }else{
			  String topologyName = "cuijh_7";
			  String topic_id = "pvlogtest";
			  String spout_id = "cuijh_3";
			  String kafkaZookeeper ="10.168.100.182:2181,10.168.100.183:2181,10.168.100.184:2181";
			  LocalCluster cluster = new LocalCluster();
			  System.out.println("########################################");
			  System.out.println("########## LocalCluster Start ##########");
			  System.out.println("########################################");
			 
	    	  cluster.submitTopology(topologyName, conf, buildTopology(topologyName,topic_id,spout_id,kafkaZookeeper));
		  }
		      }
	
}



