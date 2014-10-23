package storm.kafka.trident;

import java.util.Map;
import java.util.UUID;
import storm.kafka.Partition;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ITridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class StormKafkaTridentSpout implements IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map>{
	   TridentKafkaConfig _config;
	    String _topologyInstanceId = UUID.randomUUID().toString();
	    public StormKafkaTridentSpout(TridentKafkaConfig config){
	    	this._config = config;
	    }
	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new ITridentCoordinator(conf, _config);
	}

	@Override
	public Emitter getEmitter(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new ITridentEmitter(conf,context, _config, _topologyInstanceId);
	}

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return _config.scheme.getOutputFields();
	}
	
}
