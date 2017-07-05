package xue.trident.word;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

/**
 * 数据源
 * @author Administrator
 *
 */
public class SubjectsSpout implements IBatchSpout{

	private static final long serialVersionUID = 1L;

	
	//批处理大小
	private int batchSize;
	//容器
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();
	
	
	public SubjectsSpout(int batchSize) {
		this.batchSize = batchSize;
	}
	
	private static final Map<Integer,String> DATA_MAP = new HashMap<Integer,String>();

	static{
		DATA_MAP.put(0, "java java php ruby c++");
		DATA_MAP.put(1, "java python python python c++");
		DATA_MAP.put(2, "java java java java ruby");
		DATA_MAP.put(3, "c++ java ruby php java");
	}
	
	@Override
	public void open(Map conf, TopologyContext context) {
		
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = new ArrayList<List<Object>>();
		for (int i = 0; i < this.batchSize; i++) {
			batches.add(new Values(DATA_MAP.get(i)));
		}
		System.out.println("batchID:" + batchId);
		this.batchesMap.put(batchId, batches);
		for (List<Object> list : batches) {
			collector.emit(list);
		}
	}
	
	@Override
	public Fields getOutputFields() {
		return new Fields("subjects");
	}
	
	@Override
	public void ack(long batchId) {
		System.out.println("remove batchId:" + batchId);
		this.batchesMap.remove(batchId);
	}
	
	@Override
	public Map getComponentConfiguration() {
		return null;
	}
	
	@Override
	public void close() {
		
	}

	

	

}
