package storm.kafka.trident;
import java.util.Map;






import storm.kafka.KafkaUtils;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;
public class ITridentCoordinator implements IPartitionedTridentSpout.Coordinator<GlobalPartitionInformation>, IOpaquePartitionedTridentSpout.Coordinator<GlobalPartitionInformation>{
	private IBrokerReader reader;
    private TridentKafkaConfig config;

	public ITridentCoordinator(Map conf, TridentKafkaConfig tridentKafkaConfig){
	    config = tridentKafkaConfig;
        reader = KafkaUtils.makeBrokerReader(conf, config);
	}
	  @Override
      public GlobalPartitionInformation getPartitionsForBatch() {
              return reader.getCurrentBrokers();
      }
	@Override
	public boolean isReady(long txid) {
		// TODO Auto-generated method stub
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
