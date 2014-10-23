package storm.kafka.trident;



import java.net.ConnectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableMap;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.KafkaUtils;
import storm.kafka.Partition;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;



public class ITridentEmitter implements IPartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map>{
	  public static final Logger LOG = LoggerFactory.getLogger(ITridentEmitter.class);
	   private DynamicPartitionConnections _connections;
	   private String _topologyName;
	   private TridentKafkaConfig _config;
       private String _topologyInstanceId;
       private KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;

	public ITridentEmitter(Map conf, TopologyContext context,TridentKafkaConfig _config2, String topologyInstanceId){
			this._config = _config2;
			_topologyInstanceId = topologyInstanceId;
			 _connections = new DynamicPartitionConnections(_config, KafkaUtils.makeBrokerReader(conf, _config));
			 _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
			 _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
             context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
             LOG.info(" Trident Kafka Emitter new instance is running ");
	}


	@Override
	public List<Partition> getOrderedPartitions(GlobalPartitionInformation allPartitionInfo) {

		return allPartitionInfo.getOrderedPartitions();
	}


	/**
	 * TrasactionalTopolgy里发送的tuple都必须以TransactionAttempt作为第一个field,Storm根据这个field来判断
	 * tuple属于哪一个batch
	 * TransactionAttempt包含两个值: 一个transaction id, 一个attempt id, transaction id的作用就是每个batch的中tuple是唯一的
	 */
	@Override
	public Map emitPartitionBatchNew(TransactionAttempt tx,TridentCollector collector, Partition partition,Map lastPartitionMeta) {
		// TODO Auto-generated method stub
		return failFastEmitNewPartitionBatch(tx, collector, partition, lastPartitionMeta);
	}


	@Override
	public void refreshPartitions(List<Partition> partitionResponsibilities) {

		 _connections.clear();
         _kafkaOffsetMetric.refreshPartitions(new HashSet<Partition>(partitionResponsibilities));
	}


	@Override
	public void emitPartitionBatch(TransactionAttempt tx,TridentCollector collector, Partition partition, Map partitionMeta) {
		LOG.info("re-emmint ");
		reEmitPartitionBatch(tx, collector, partition, partitionMeta);
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		_connections.clear();
	}
	private Map failFastEmitNewPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition, Map lastMeta) {
        SimpleConsumer consumer = _connections.register(partition);
        Long transactionId =  attempt.getTransactionId();
        //从这里进入emit
        Map ret = doEmitNewPartitionBatch(transactionId,consumer, partition, collector, lastMeta);
        _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long) ret.get("offset"));
        return ret;
	}
	 private Map doEmitNewPartitionBatch(Long transaction_Id, SimpleConsumer consumer, Partition partition, TridentCollector collector, Map lastMeta) {
         long offset;
         if (lastMeta != null) {
                 String lastInstanceId = null;
                 Map lastTopoMeta = (Map) lastMeta.get("topology");
                 if (lastTopoMeta != null) {
                         lastInstanceId = (String) lastTopoMeta.get("id");
                 }
                 if (_config.forceFromStart && !_topologyInstanceId.equals(lastInstanceId)) {
                	 LOG.info("@@@ lastMeat != null  partition.partition="+partition.partition);
                         offset = KafkaUtils.getOffset(consumer, _config.topic, partition.partition, _config.startOffsetTime);
                 } else {
                         offset = (Long) lastMeta.get("nextOffset");
                 }
         } else {
                 long startTime = kafka.api.OffsetRequest.LatestTime();
                 if (_config.forceFromStart) startTime = _config.startOffsetTime;
                 offset = KafkaUtils.getOffset(consumer, _config.topic, partition.partition, startTime);
                 LOG.info("@@@ lastMeat == null  partition.partition="+partition.partition);
                 LOG.info(partition.partition+" *********   is "+offset);
                 System.out.println();
         }
         ByteBufferMessageSet msgs = null;
         try {
                 msgs = fetchMessages(consumer, partition, 0);
         } catch (Exception e) {
                 if (e instanceof ConnectException) {
                	 LOG.error("connection exception ");
//                         throw new FailedFetchException(e);
                 } else {
                         throw new RuntimeException(e);
                 }
         }
         long endoffset = offset;
         for (MessageAndOffset msg : msgs) {
        	 	LOG.info("&&& message is "+msg.message().toString());
                 emit(collector, msg.message(),transaction_Id,partition.partition,endoffset);
                 endoffset = msg.nextOffset();
         }

         LOG.info("partition.partition = "+partition.partition+" ^^^  endoffset = "+endoffset);
         Map newMeta = new HashMap();
         newMeta.put("offset", offset);
         newMeta.put("nextOffset", endoffset);
         newMeta.put("instanceId", _topologyInstanceId);
         newMeta.put("partition", partition.partition);
         newMeta.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
         newMeta.put("topic", _config.topic);
         newMeta.put("topology", ImmutableMap.of("name", _topologyName, "id", _topologyInstanceId));
         return newMeta;
 }
	  private ByteBufferMessageSet fetchMessages(SimpleConsumer consumer, Partition partition, long offset) {
          ByteBufferMessageSet msgs;
          long start = System.nanoTime();
          FetchRequestBuilder builder = new FetchRequestBuilder();
          FetchRequest fetchRequest = builder.addFetch(_config.topic, partition.partition, offset, _config.fetchSizeBytes).clientId(_config.clientId).build();
          msgs = consumer.fetch(fetchRequest).messageSet(_config.topic, partition.partition);
          long end = System.nanoTime();
          long millis = (end - start) / 1000000;

          return msgs;
  }
	   private void emit(TridentCollector collector, Message msg, Long transaction_Id, int partition, long endoffset) {
           Iterable<List<Object>> values =  _config.scheme.deserialize(Utils.toByteArray(msg.payload()));
           if (values != null) {
                   for (List<Object> value : values) {
                   		String txdata =transaction_Id+"@"+partition+"@"+endoffset+"&";
                   		value.set(0, txdata+value.get(0));
                   		LOG.info(value.toString());
                           collector.emit(value);
                   }
           }
   }
	   private void reEmitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition, Map meta) {
           LOG.info("re-emitting batch, attempt " + attempt);
           String instanceId = (String) meta.get("instanceId");
           if (!_config.forceFromStart || instanceId.equals(_topologyInstanceId)) {
                   SimpleConsumer consumer = _connections.register(partition);
                   long offset = (Long) meta.get("offset");
                   long nextOffset = (Long) meta.get("nextOffset");
                   ByteBufferMessageSet msgs = fetchMessages(consumer, partition, offset);
                   for (MessageAndOffset msg : msgs) {
                           if (offset == nextOffset) break;
                           if (offset > nextOffset) {
                                   throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                           }
                           emit(collector, msg.message(),attempt.getTransactionId(),partition.partition,Integer.parseInt(meta.get("nextOffset").toString()));
                           LOG.info("", "msg::" + msg.message());
                           offset = msg.nextOffset();
                   }
           }
   }
}
