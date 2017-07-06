package xue.redis.storm;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class RedisTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		List<String> zks = new ArrayList<String>();
		zks.add("192.168.1.191");
		
		List<String> cFs = new ArrayList<String>();
		cFs.add("personal");
		cFs.add("company");
		
		//set the spout class
		builder.setSpout("spout", new SampleSpout(),2);
		//set the bolt class
		builder.setBolt("bolt", new StormRedisBolt("192.168.1.191",6379),2).shuffleGrouping("spout");
	
		Config conf = new Config();
		conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("StormRedisTopology", conf, builder.createTopology());;
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.out.println("Thread interrupted execption : " + e);
		}
		
		//kill the StormRedisTopology
		cluster.killTopology("StormRedisTopology");
		//shutdown the Storm test cluster
		cluster.shutdown();
	}
}
